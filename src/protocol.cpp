#include "protocol.hpp"

#include <cctype>
#include <limits>
#include <sstream>
#include <string_view>

namespace peadb {
namespace {

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

std::optional<ParsedCommand> parse_resp_array(std::string_view buffer) {
  if (buffer.empty() || buffer[0] != '*') return std::nullopt;

  const auto line_end = buffer.find("\r\n");
  if (line_end == std::string::npos) return std::nullopt;

  int count = 0;
  if (!parse_i32(buffer.substr(1, line_end - 1), count)) {
    return ParsedCommand{{"__parse_error__"}, line_end + 2};
  }

  if (count < 0) return ParsedCommand{{"__parse_error__"}, line_end + 2};

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

std::optional<ParsedCommand> parse_inline(std::string_view buffer) {
  const auto line_end = buffer.find("\r\n");
  if (line_end == std::string::npos) return std::nullopt;

  const std::string line(buffer.substr(0, line_end));
  std::istringstream iss(line);
  std::vector<std::string> args;
  std::string token;
  while (iss >> token) {
    args.push_back(token);
  }

  if (args.empty()) {
    return ParsedCommand{{"__empty__"}, line_end + 2};
  }

  return ParsedCommand{std::move(args), line_end + 2};
}

}  // namespace

std::optional<ParsedCommand> parse_one_command(std::string_view buffer) {
  if (buffer.empty()) return std::nullopt;
  if (buffer[0] == '*') return parse_resp_array(buffer);
  if (std::isprint(static_cast<unsigned char>(buffer[0]))) return parse_inline(buffer);
  return std::nullopt;
}

}  // namespace peadb
