#include "protocol.hpp"

#include <cctype>
#include <limits>
#include <sstream>
#include <string_view>

namespace peadb {
namespace {

// Protocol limits matching Redis defaults. Enforced *before* allocating, so a tiny
// header (e.g. "*2000000000\r\n") cannot trigger a huge reservation / OOM crash.
constexpr int kMaxMultibulk = 1024 * 1024;            // 1M elements
constexpr int kMaxBulkLen   = 512 * 1024 * 1024;      // 512 MB (proto-max-bulk-len)
// Tighter caps for clients that have not authenticated when a password is required
// (Redis processMultibulkBuffer): >10 elements or >16 KB per bulk is refused up front.
constexpr int kUnauthMaxMultibulk = 10;
constexpr int kUnauthMaxBulkLen   = 16 * 1024;

bool parse_i32(std::string_view sv, int& out) {
  if (sv.empty()) return false;
  bool negative = false;
  std::size_t i = 0;
  if (sv[0] == '-') {
    negative = true;
    i = 1;
    if (i == sv.size()) return false;
  }
  int value = 0;
  for (; i < sv.size(); ++i) {
    const char c = sv[i];
    if (c < '0' || c > '9') return false;
    const int digit = c - '0';
    if (value > (std::numeric_limits<int>::max() - digit) / 10) return false;
    value = value * 10 + digit;
  }
  out = negative ? -value : value;
  return true;
}

std::optional<ParsedCommand> parse_resp_array(std::string_view buffer, bool unauthenticated) {
  if (buffer.empty() || buffer[0] != '*') return std::nullopt;

  const auto line_end = buffer.find("\r\n");
  if (line_end == std::string::npos) return std::nullopt;

  int count = 0;
  if (!parse_i32(buffer.substr(1, line_end - 1), count)) {
    return ParsedCommand{{"__parse_error__"}, line_end + 2};
  }

  if (count < 0) return ParsedCommand{{"__parse_error__"}, line_end + 2};
  if (count > kMaxMultibulk) return ParsedCommand{{"__err_multibulk__"}, line_end + 2};
  if (unauthenticated && count > kUnauthMaxMultibulk)
    return ParsedCommand{{"__err_unauth_multibulk__"}, line_end + 2};

  std::size_t pos = line_end + 2;
  std::vector<std::string> args;
  args.reserve(static_cast<std::size_t>(count));

  for (int i = 0; i < count; ++i) {
    if (pos >= buffer.size() || buffer[pos] != '$') return std::nullopt;
    const auto bulk_end = buffer.find("\r\n", pos);
    if (bulk_end == std::string::npos) return std::nullopt;

    int len = 0;
    if (!parse_i32(buffer.substr(pos + 1, bulk_end - pos - 1), len)) {
      return ParsedCommand{{"__parse_error__"}, bulk_end + 2};
    }

    if (len < 0) return ParsedCommand{{"__parse_error__"}, bulk_end + 2};
    if (len > kMaxBulkLen) return ParsedCommand{{"__err_bulklen__"}, bulk_end + 2};
    if (unauthenticated && len > kUnauthMaxBulkLen)
      return ParsedCommand{{"__err_unauth_bulklen__"}, bulk_end + 2};

    const std::size_t data_start = bulk_end + 2;
    const std::size_t required = data_start + static_cast<std::size_t>(len) + 2;
    if (required > buffer.size()) return std::nullopt;
    if (buffer[required - 2] != '\r' || buffer[required - 1] != '\n') {
      return ParsedCommand{{"__parse_error__"}, required};
    }

    args.emplace_back(buffer.substr(data_start, static_cast<std::size_t>(len)));
    pos = required;
  }

  return ParsedCommand{std::move(args), pos};
}

// Split an inline command line honoring quotes and escapes, matching Redis's
// sdssplitargs: double quotes allow \xHH and C-style escapes (\n,\r,\t,\b,\a),
// single quotes are literal except \', and unbalanced quotes are a protocol error.
// Returns false on a malformed (unbalanced-quote) line.
bool split_inline_args(const std::string& line, std::vector<std::string>& out) {
  std::size_t i = 0;
  const std::size_t n = line.size();
  auto hexdigit = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
  };
  while (true) {
    while (i < n && std::isspace(static_cast<unsigned char>(line[i]))) ++i;  // skip blanks
    if (i >= n) break;
    std::string cur;
    bool done = false;
    while (!done) {
      if (line[i] == '"') {
        ++i;
        while (i < n && line[i] != '"') {
          if (line[i] == '\\' && i + 1 < n) {
            char e = line[i + 1];
            if (e == 'x' && i + 3 < n && hexdigit(line[i + 2]) >= 0 && hexdigit(line[i + 3]) >= 0) {
              cur.push_back(static_cast<char>(hexdigit(line[i + 2]) * 16 + hexdigit(line[i + 3])));
              i += 4;
            } else {
              switch (e) {
                case 'n': cur.push_back('\n'); break;
                case 'r': cur.push_back('\r'); break;
                case 't': cur.push_back('\t'); break;
                case 'b': cur.push_back('\b'); break;
                case 'a': cur.push_back('\a'); break;
                default:  cur.push_back(e); break;
              }
              i += 2;
            }
          } else {
            cur.push_back(line[i++]);
          }
        }
        if (i >= n) return false;  // unterminated quote
        ++i;  // closing quote — must be followed by space or EOL
      } else if (line[i] == '\'') {
        ++i;
        while (i < n && line[i] != '\'') {
          if (line[i] == '\\' && i + 1 < n && line[i + 1] == '\'') { cur.push_back('\''); i += 2; }
          else cur.push_back(line[i++]);
        }
        if (i >= n) return false;  // unterminated quote
        ++i;
      } else {
        cur.push_back(line[i++]);
      }
      if (i >= n || std::isspace(static_cast<unsigned char>(line[i]))) done = true;
    }
    out.push_back(std::move(cur));
  }
  return true;
}

std::optional<ParsedCommand> parse_inline(std::string_view buffer) {
  const auto line_end = buffer.find("\r\n");
  if (line_end == std::string::npos) return std::nullopt;

  const std::string line(buffer.substr(0, line_end));
  std::vector<std::string> args;
  if (!split_inline_args(line, args)) {
    // Unbalanced quotes — Redis returns a protocol error and closes the connection.
    return ParsedCommand{{"__parse_error__"}, line_end + 2};
  }

  if (args.empty()) {
    return ParsedCommand{{"__empty__"}, line_end + 2};
  }

  return ParsedCommand{std::move(args), line_end + 2};
}

}  // namespace

std::optional<ParsedCommand> parse_one_command(std::string_view buffer, bool unauthenticated) {
  if (buffer.empty()) return std::nullopt;
  if (buffer[0] == '*') return parse_resp_array(buffer, unauthenticated);
  if (std::isprint(static_cast<unsigned char>(buffer[0]))) return parse_inline(buffer);
  return std::nullopt;
}

}  // namespace peadb
