#include "protocol.hpp"

#include <cctype>
#include <limits>
#include <sstream>
#include <string_view>

namespace peadb {
namespace {

// Protocol limits matching Redis defaults. Enforced *before* allocating, so a tiny
// header (e.g. "*2000000000\r\n") cannot trigger a huge reservation / OOM crash.
// Redis caps the multibulk count at INT_MAX, but Redis's parser is *stateful* (it never
// re-scans consumed bytes). PeaDB re-parses the whole unconsumed buffer on every read, so an
// enormous declared count with dribbled, never-completing elements would re-scan quadratically
// up to the query-buffer limit. This deliberately-stricter 1M cap bounds that; no realistic
// client or the upstream suite ever declares more.
constexpr int kMaxMultibulk = 1024 * 1024;            // 1M elements
// The bulk length cap is proto-max-bulk-len, which the caller passes in (CONFIG SET changes it).
// Tighter caps for clients that have not authenticated when a password is required
// (Redis processMultibulkBuffer): >10 elements or >16 KB per bulk is refused up front.
constexpr int kUnauthMaxMultibulk = 10;
constexpr int kUnauthMaxBulkLen   = 16 * 1024;
// PROTO_INLINE_MAX_SIZE (server.h): a single line (inline command, multibulk count, or bulk
// length) that reaches this size without a terminator is a desync — error out and close
// rather than buffering unboundedly.
constexpr std::size_t kProtoInlineMaxSize = 64 * 1024;

template <typename Int>
bool parse_int(std::string_view sv, Int& out) {
  if (sv.empty()) return false;
  bool negative = false;
  std::size_t i = 0;
  if (sv[0] == '-') {
    negative = true;
    i = 1;
    if (i == sv.size()) return false;
  }
  Int value = 0;
  for (; i < sv.size(); ++i) {
    const char c = sv[i];
    if (c < '0' || c > '9') return false;
    const Int digit = c - '0';
    if (value > (std::numeric_limits<Int>::max() - digit) / 10) return false;
    value = value * 10 + digit;
  }
  out = negative ? -value : value;
  return true;
}

// A protocol error whose message is carried in args[1] (handle_command prefixes "Protocol
// error: " and closes the connection). Keeps the wording exactly Redis's.
inline ParsedCommand proto_error(std::string msg, std::size_t consumed) {
  return ParsedCommand{{"__err_proto__", std::move(msg)}, consumed};
}

std::optional<ParsedCommand> parse_resp_array(std::string_view buffer, bool unauthenticated,
                                              std::int64_t max_bulk_len) {
  if (buffer.empty() || buffer[0] != '*') return std::nullopt;

  // ── Multibulk count line: *<count>\r\n ──
  const auto line_end = buffer.find("\r\n");
  if (line_end == std::string::npos) {
    if (buffer.size() > kProtoInlineMaxSize)
      return proto_error("too big mbulk count string", buffer.size());
    return std::nullopt;  // need more data
  }

  int count = 0;
  // parse_int fails on a non-number OR on overflow past INT_MAX — both are Redis's
  // "invalid multibulk length" case; the stateless-parser bound (kMaxMultibulk) is the same.
  if (!parse_int(buffer.substr(1, line_end - 1), count) || count > kMaxMultibulk)
    return ParsedCommand{{"__err_multibulk__"}, line_end + 2};
  // The unauthenticated cap is checked before the <=0 skip, matching Redis's order.
  if (unauthenticated && count > kUnauthMaxMultibulk)
    return ParsedCommand{{"__err_unauth_multibulk__"}, line_end + 2};
  // A non-positive count is a no-op: consume the line and produce no reply (e.g. *-1\r\n).
  if (count <= 0) return ParsedCommand{{"__empty__"}, line_end + 2};

  std::size_t pos = line_end + 2;
  std::vector<std::string> args;
  args.reserve(static_cast<std::size_t>(std::min(count, 1024)));

  for (int i = 0; i < count; ++i) {
    if (pos >= buffer.size()) return std::nullopt;  // bulk header not arrived yet
    // Each element must be a bulk string; anything else is a desync.
    if (buffer[pos] != '$')
      return proto_error(std::string("expected '$', got '") + buffer[pos] + "'", pos);

    const auto bulk_end = buffer.find("\r\n", pos);
    if (bulk_end == std::string::npos) {
      if (buffer.size() - pos > kProtoInlineMaxSize)
        return proto_error("too big bulk count string", buffer.size());
      return std::nullopt;  // need more data
    }

    std::int64_t len = 0;
    if (!parse_int(buffer.substr(pos + 1, bulk_end - pos - 1), len) || len < 0 ||
        len > max_bulk_len)
      return ParsedCommand{{"__err_bulklen__"}, bulk_end + 2};
    if (unauthenticated && len > kUnauthMaxBulkLen)
      return ParsedCommand{{"__err_unauth_bulklen__"}, bulk_end + 2};

    const std::size_t data_start = bulk_end + 2;
    const std::size_t required = data_start + static_cast<std::size_t>(len) + 2;
    if (required > buffer.size()) return std::nullopt;  // argument body not arrived yet
    // Redis does not content-validate the two trailing bytes; it just skips them.
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
        ++i;  // consume closing quote
        // Redis sdssplitargs: a closing quote must be followed by whitespace or end-of-line;
        // e.g. `"""x"""` is unbalanced, not three concatenated tokens.
        if (i < n && !std::isspace(static_cast<unsigned char>(line[i]))) return false;
      } else if (line[i] == '\'') {
        ++i;
        while (i < n && line[i] != '\'') {
          if (line[i] == '\\' && i + 1 < n && line[i + 1] == '\'') { cur.push_back('\''); i += 2; }
          else cur.push_back(line[i++]);
        }
        if (i >= n) return false;  // unterminated quote
        ++i;  // consume closing quote
        if (i < n && !std::isspace(static_cast<unsigned char>(line[i]))) return false;
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
  // Redis terminates an inline line at the first '\n', tolerating an optional preceding '\r'
  // (both "\r\n" and a bare "\n" are accepted).
  const auto nl = buffer.find('\n');
  if (nl == std::string::npos) {
    if (buffer.size() > kProtoInlineMaxSize)
      return proto_error("too big inline request", buffer.size());
    return std::nullopt;  // need more data
  }
  std::size_t line_len = nl;
  if (line_len > 0 && buffer[line_len - 1] == '\r') --line_len;
  const std::size_t consumed = nl + 1;

  const std::string line(buffer.substr(0, line_len));
  std::vector<std::string> args;
  if (!split_inline_args(line, args))
    return proto_error("unbalanced quotes in request", consumed);

  // An empty (or whitespace-only) inline line is silently ignored — no reply, no close.
  if (args.empty()) return ParsedCommand{{"__empty__"}, consumed};

  return ParsedCommand{std::move(args), consumed};
}

}  // namespace

std::optional<ParsedCommand> parse_one_command(std::string_view buffer, bool unauthenticated,
                                               std::int64_t max_bulk_len) {
  if (buffer.empty()) return std::nullopt;
  // Redis (processInputBuffer): first byte '*' → multibulk, anything else → inline.
  if (buffer[0] == '*') return parse_resp_array(buffer, unauthenticated, max_bulk_len);
  return parse_inline(buffer);
}

}  // namespace peadb
