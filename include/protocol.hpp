#pragma once

#include <cstdint>
#include <limits>
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
// `max_bulk_len` is proto-max-bulk-len for a client connection; Redis applies no such cap to
// its master link or to an AOF it loads, which keep the default.
std::optional<ParsedCommand> parse_one_command(
    std::string_view buffer, bool unauthenticated = false,
    std::int64_t max_bulk_len = std::numeric_limits<std::int64_t>::max());

}  // namespace peadb
