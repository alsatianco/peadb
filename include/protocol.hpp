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

// When `unauthenticated` is true (a password is required and the client has not yet
// authenticated), Redis applies tighter protocol limits — multibulk count > 10 and bulk
// length > 16KB are rejected before the frame is buffered — so an unauthenticated peer
// cannot pin large amounts of memory. See processMultibulkBuffer / authRequired().
std::optional<ParsedCommand> parse_one_command(std::string_view buffer, bool unauthenticated = false);

} // namespace peadb
