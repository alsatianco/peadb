// Config file and command-line loader. It follows Redis 7.2.5's loadServerConfigFromString()
// (src/config.c): the same line splitting, the same settings and aliases (generated from
// config.c into redis_config_table.generated.hpp), the same value checks and the same errors.
// A Redis setting that PeaDB cannot honour safely stops startup; any other Redis setting that
// PeaDB does not apply yet is accepted with a warning (compat/delta.md item 24). CONFIG SET
// (command.cpp) runs the same value checks through check_redis_config_value().
#include "config.hpp"

#include "command_metadata.generated.hpp"
#include "peadb_version.hpp"
#include "redis_config_table.generated.hpp"

#include <glob.h>
#include <locale.h>
#include <strings.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <unordered_set>

namespace peadb {
namespace {

// Redis has no limit; this only turns an include loop into an error instead of a stack overflow.
constexpr int kMaxIncludeDepth = 32;
constexpr std::size_t kMaxBindAddresses = 16;  // CONFIG_BINDADDR_MAX (server.h)

std::string to_lower(std::string s) {
  for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

bool iequals(const std::string& a, const char* b) { return strcasecmp(a.c_str(), b) == 0; }

// sdstrim(line, " \t\r\n")
std::string trim_line(const std::string& s) {
  const char* ws = " \t\r\n";
  const auto first = s.find_first_not_of(ws);
  if (first == std::string::npos) return "";
  return s.substr(first, s.find_last_not_of(ws) - first + 1);
}

bool is_hex_digit(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

int hex_digit_to_int(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  return c - 'A' + 10;
}

// sdscatrepr(): a quoted copy of `s` that split_config_args() turns back into `s`.
std::string quote_arg(const std::string& s) {
  std::string out = "\"";
  for (const char ch : s) {
    const auto c = static_cast<unsigned char>(ch);
    switch (c) {
      case '\\': out += "\\\\"; break;
      case '"': out += "\\\""; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      case '\a': out += "\\a"; break;
      case '\b': out += "\\b"; break;
      default:
        if (std::isprint(c)) {
          out.push_back(ch);
        } else {
          char buf[5];
          std::snprintf(buf, sizeof(buf), "\\x%02x", c);
          out += buf;
        }
    }
  }
  return out + "\"";
}

// string2ll() (util.c): a strict decimal integer — no spaces, no '+', no leading zeros.
bool string2ll(const std::string& s, long long& value) {
  constexpr std::size_t kLongStrSize = 21;  // LONG_STR_SIZE
  const std::size_t slen = s.size();
  if (slen == 0 || slen >= kLongStrSize) return false;
  if (slen == 1 && s[0] == '0') {
    value = 0;
    return true;
  }
  std::size_t i = 0;
  bool negative = false;
  if (s[0] == '-') {
    negative = true;
    if (++i == slen) return false;
  }
  if (s[i] < '1' || s[i] > '9') return false;
  unsigned long long v = static_cast<unsigned long long>(s[i++] - '0');
  for (; i < slen && s[i] >= '0' && s[i] <= '9'; ++i) {
    if (v > ULLONG_MAX / 10) return false;
    v *= 10;
    const auto d = static_cast<unsigned long long>(s[i] - '0');
    if (v > ULLONG_MAX - d) return false;
    v += d;
  }
  if (i < slen) return false;
  if (negative) {
    if (v > static_cast<unsigned long long>(-(LLONG_MIN + 1)) + 1) return false;
    value = static_cast<long long>(0ULL - v);
  } else {
    if (v > static_cast<unsigned long long>(LLONG_MAX)) return false;
    value = static_cast<long long>(v);
  }
  return true;
}

// memtoull() (util.c): a byte count with an optional unit (b, k, kb, m, mb, g, gb; any case).
bool memtoull(const std::string& s, unsigned long long& value) {
  const char* p = s.c_str();
  if (*p == '-') return false;
  const char* u = p;
  while (*u && std::isdigit(static_cast<unsigned char>(*u))) ++u;
  unsigned long long mul;
  if (*u == '\0' || !strcasecmp(u, "b")) mul = 1;
  else if (!strcasecmp(u, "k")) mul = 1000;
  else if (!strcasecmp(u, "kb")) mul = 1024;
  else if (!strcasecmp(u, "m")) mul = 1000 * 1000;
  else if (!strcasecmp(u, "mb")) mul = 1024 * 1024;
  else if (!strcasecmp(u, "g")) mul = 1000ULL * 1000 * 1000;
  else if (!strcasecmp(u, "gb")) mul = 1024ULL * 1024 * 1024;
  else return false;
  const std::size_t digits = static_cast<std::size_t>(u - p);
  if (digits >= 128) return false;
  const std::string buf(p, digits);
  char* endptr = nullptr;
  errno = 0;
  const unsigned long long val = std::strtoull(buf.c_str(), &endptr, 10);
  if ((val == 0 && errno == EINVAL) || *endptr != '\0') return false;
  value = val * mul;
  return true;
}

// string2d() (util.c)
bool string2d(const std::string& s, double& d) {
  errno = 0;
  char* eptr = nullptr;
  d = std::strtod(s.c_str(), &eptr);
  if (s.empty() || std::isspace(static_cast<unsigned char>(s[0])) ||
      static_cast<std::size_t>(eptr - s.c_str()) != s.size() ||
      (errno == ERANGE && (d == HUGE_VAL || d == -HUGE_VAL || std::fpclassify(d) == FP_ZERO)) ||
      std::isnan(d)) {
    return false;
  }
  return true;
}

// Redis's pathIsBaseName() (util.c)
bool path_is_base_name(const std::string& s) {
  return s.find('/') == std::string::npos && s.find('\\') == std::string::npos;
}

std::string join(const std::vector<std::string>& args) {
  std::string out;
  for (const auto& a : args) {
    if (!out.empty()) out.push_back(' ');
    out += a;
  }
  return out;
}

// ── Value checks, as Redis's config set functions do them ──

bool check_bool(const RedisConfigSpec& spec, const std::string& v, std::string& canonical,
                std::string& err) {
  if (iequals(v, "yes")) canonical = "yes";
  else if (iequals(v, "no")) canonical = "no";
  else {
    err = "argument must be 'yes' or 'no'";
    return false;
  }
  // isValidActiveDefrag(): PeaDB has no defragmentation, like a Redis built without HAVE_DEFRAG
  // (the pinned Redis 7.2.5 built with libc malloc gives the same answer).
  if (canonical == "yes" && spec.is_valid != nullptr &&
      std::strcmp(spec.is_valid, "isValidActiveDefrag") == 0) {
    err = "Active defragmentation cannot be enabled: it requires a Redis server compiled with a "
          "modified Jemalloc like the one shipped by default with the Redis source distribution";
    return false;
  }
  return true;
}

bool check_enum(const RedisConfigSpec& spec, const std::vector<std::string>& args,
                std::string& canonical, std::string& err) {
  // configEnumGetValue(): one value, or any number of them for a MULTI_ARG (bit flag) enum.
  bool ok = !args.empty() && (spec.multi_arg || args.size() == 1);
  std::vector<std::string> names;
  for (std::size_t i = 0; ok && i < args.size(); ++i) {
    const char* match = nullptr;
    for (auto e = spec.enum_names; *e != nullptr; ++e) {
      if (iequals(args[i], *e)) match = *e;
    }
    if (match == nullptr) ok = false;
    else names.emplace_back(match);
  }
  if (!ok) {
    err = "argument(s) must be one of the following: ";
    for (auto e = spec.enum_names; *e != nullptr; ++e) {
      if (e != spec.enum_names) err += ", ";
      err += *e;
    }
    return false;
  }
  if (spec.is_valid != nullptr && std::strcmp(spec.is_valid, "isValidShutdownOnSigFlags") == 0) {
    bool save = false, nosave = false;
    for (const auto& n : names) {
      save = save || n == "save";
      nosave = nosave || n == "nosave";
    }
    if (save && nosave) {
      err = "shutdown options SAVE and NOSAVE can't be used simultaneously";
      return false;
    }
  }
  if (spec.multi_arg) {
    // A MULTI_ARG enum is a set of bit flags, and configEnumGetName() prints the chosen flags in
    // the enum's order, each once; its first entry has the value 0 ("default") and is printed
    // only when nothing else is chosen.
    std::vector<std::string> ordered;
    for (auto e = spec.enum_names + 1; *e != nullptr; ++e) {
      if (std::find(names.begin(), names.end(), *e) != names.end()) ordered.emplace_back(*e);
    }
    if (ordered.empty()) ordered.emplace_back(spec.enum_names[0]);
    names = std::move(ordered);
  }
  canonical = join(names);
  return true;
}

bool check_numeric(const RedisConfigSpec& spec, const std::string& v, long long& ll,
                   std::string& err) {
  // numericParseString()
  bool parsed = false;
  if ((spec.numeric_flags & kNumMemory) != 0) {
    unsigned long long u = 0;
    if (memtoull(v, u)) {
      ll = static_cast<long long>(u);
      parsed = true;
    }
  }
  if (!parsed && (spec.numeric_flags & kNumPercent) != 0 && v.size() > 1 && v.back() == '%' &&
      string2ll(v.substr(0, v.size() - 1), ll) && ll >= 0) {
    ll = -ll;  // Redis stores a percentage as a negative value
    parsed = true;
  }
  if (!parsed && (spec.numeric_flags & kNumOctal) != 0) {
    char* endptr = nullptr;
    errno = 0;
    ll = std::strtoll(v.c_str(), &endptr, 8);
    parsed = errno == 0 && *endptr == '\0';
  }
  if (!parsed && spec.numeric_flags == 0) parsed = string2ll(v, ll);
  if (!parsed) {
    if ((spec.numeric_flags & kNumMemory) != 0 && (spec.numeric_flags & kNumPercent) != 0)
      err = "argument must be a memory or percent value";
    else if ((spec.numeric_flags & kNumMemory) != 0)
      err = "argument must be a memory value";
    else if ((spec.numeric_flags & kNumOctal) != 0)
      err = "argument couldn't be parsed as an octal number";
    else
      err = "argument couldn't be parsed into an integer";
    return false;
  }
  // numericBoundaryCheck()
  char buf[128];
  if (spec.numeric_unsigned) {
    const auto ull = static_cast<unsigned long long>(ll);
    const auto lo = static_cast<unsigned long long>(spec.lower);
    const auto hi = static_cast<unsigned long long>(spec.upper);
    if (ull > hi || ull < lo) {
      if ((spec.numeric_flags & kNumOctal) != 0)
        std::snprintf(buf, sizeof(buf), "argument must be between %llo and %llo inclusive", lo, hi);
      else
        std::snprintf(buf, sizeof(buf), "argument must be between %llu and %llu inclusive", lo, hi);
      err = buf;
      return false;
    }
  } else if ((spec.numeric_flags & kNumPercent) != 0 && ll < 0) {
    if (ll < spec.lower) {
      std::snprintf(buf, sizeof(buf), "percentage argument must be less or equal to %lld",
                    -spec.lower);
      err = buf;
      return false;
    }
  } else if (ll > spec.upper || ll < spec.lower) {
    std::snprintf(buf, sizeof(buf), "argument must be between %lld and %lld inclusive",
                  spec.lower, spec.upper);
    err = buf;
    return false;
  }
  return true;
}

bool check_string(const RedisConfigSpec& spec, const std::string& v, std::string& err) {
  if (spec.is_valid == nullptr) return true;
  const std::string fn = spec.is_valid;
  if (fn == "isValidDBfilename" && !path_is_base_name(v)) {
    err = "dbfilename can't be a path, just a filename";
    return false;
  }
  if (fn == "isValidAOFfilename" || fn == "isValidAOFdirname") {
    const bool dir = fn == "isValidAOFdirname";
    if (v.empty()) {
      err = dir ? "appenddirname can't be empty" : "appendfilename can't be empty";
      return false;
    }
    if (!path_is_base_name(v)) {
      err = dir ? "appenddirname can't be a path, just a dirname"
                : "appendfilename can't be a path, just a filename";
      return false;
    }
  }
  if (fn == "isValidAnnouncedHostname") {
    if (v.size() >= 256) {  // NET_HOST_STR_LEN
      err = "Hostnames must be less than 256 characters";
      return false;
    }
    for (const char c : v) {
      if (!std::isalnum(static_cast<unsigned char>(c)) && c != '-' && c != '.') {
        err = "Hostnames may only contain alphanumeric characters, hyphens or dots";
        return false;
      }
    }
  }
  if (fn == "isValidAnnouncedNodename") {
    for (const char c : v) {  // isValidAuxChar() (cluster.c)
      if (!std::isalnum(static_cast<unsigned char>(c)) &&
          std::strchr("!#$%&()*+:;<>?@[]^{|}~", c) != nullptr) {
        err = "Announced human node name contained invalid character";
        return false;
      }
    }
  }
  // isValidProcTitleTemplate is not checked: PeaDB does not set a process title, so the
  // setting is ignored with a warning whatever its value.
  return true;
}

// printf("%f") and then Redis's trimDoubleString() (util.c): no trailing zeros or dot.
std::string trim_double(double d) {
  char buf[128];
  std::string s(buf, static_cast<std::size_t>(std::snprintf(buf, sizeof(buf), "%f", d)));
  if (s.find('.') != std::string::npos) {
    s.erase(s.find_last_not_of('0') + 1);
    if (s.back() == '.') s.pop_back();
  }
  return s;
}

// Special settings: each has its own set function in config.c. On success `canonical` holds the
// checked arguments in the form the matching ServerConfig field expects.
bool check_special(const std::string& name, const std::vector<std::string>& in,
                   std::string& canonical, std::string& err) {
  if (name == "dir") {  // setConfigDirOption(); the chdir(2) itself is done by the caller
    if (in.size() != 1) {
      err = "wrong number of arguments";
      return false;
    }
    canonical = in[0];
    return true;
  }
  if (name == "save") {  // setConfigSaveOption()
    std::vector<std::string> args = in;
    if (args.size() == 1 && args[0].empty()) args.clear();
    bool ok = args.size() % 2 == 0;
    for (std::size_t j = 0; ok && j < args.size(); ++j) {
      char* eptr = nullptr;
      const long long val = std::strtoll(args[j].c_str(), &eptr, 10);
      ok = eptr[0] == '\0' && !((j & 1) == 0 && val < 1) && !((j & 1) == 1 && val < 0);
      // Redis stores the change count in an int, so a larger value wraps the same way.
      if (ok) {
        args[j] = std::to_string((j & 1) == 1 ? static_cast<long long>(static_cast<int>(val)) : val);
      }
    }
    if (!ok) {
      err = "Invalid save parameters";
      return false;
    }
    canonical = join(args);
    return true;
  }
  if (name == "client-output-buffer-limit") {  // updateClientOutputBufferLimit()
    if (in.size() % 4 != 0) {
      err = "Wrong number of arguments in buffer limit configuration.";
      return false;
    }
    std::vector<std::string> out;
    for (std::size_t j = 0; j < in.size(); j += 4) {
      std::string cls;
      if (iequals(in[j], "normal")) cls = "normal";
      else if (iequals(in[j], "slave") || iequals(in[j], "replica")) cls = "replica";
      else if (iequals(in[j], "pubsub")) cls = "pubsub";
      else {
        err = "Invalid client class specified in buffer limit configuration.";
        return false;
      }
      unsigned long long hard = 0, soft = 0;
      const bool hard_ok = memtoull(in[j + 1], hard);
      const bool soft_ok = memtoull(in[j + 2], soft);
      char* eptr = nullptr;
      const long long soft_seconds = std::strtoll(in[j + 3].c_str(), &eptr, 10);
      if (!hard_ok || !soft_ok || static_cast<int>(soft_seconds) < 0 || *eptr != '\0') {
        err = "Error in hard, soft or soft_seconds setting in buffer limit configuration.";
        return false;
      }
      out.insert(out.end(), {cls, std::to_string(hard), std::to_string(soft),
                             std::to_string(static_cast<int>(soft_seconds))});
    }
    canonical = join(out);
    return true;
  }
  if (name == "oom-score-adj-values") {  // setConfigOOMScoreAdjValuesOption()
    if (in.size() != 3) {
      err = "wrong number of arguments";
      return false;
    }
    for (const auto& a : in) {
      char* eptr = nullptr;
      const long long val = std::strtoll(a.c_str(), &eptr, 10);
      if (*eptr != '\0' || val < -2000 || val > 2000) {
        err = "Invalid oom-score-adj-values, elements must be between -2000 and 2000.";
        return false;
      }
    }
    canonical = join(in);
    return true;
  }
  if (name == "notify-keyspace-events") {  // keyspaceEventsStringToFlags() (notify.c)
    if (in.size() != 1) {
      err = "wrong number of arguments";
      return false;
    }
    if (in[0].find_first_not_of("Ag$lshzxeKEtmdn") != std::string::npos) {
      err = "Invalid event class character. Use 'Ag$lshzxeKEtmdn'.";
      return false;
    }
    canonical = in[0];
    return true;
  }
  if (name == "bind") {  // setConfigBindOption()
    if (in.size() > kMaxBindAddresses) {
      err = "Too many bind addresses specified.";
      return false;
    }
    canonical = (in.size() == 1 && in[0].empty()) ? "" : join(in);
    return true;
  }
  if (name == "replicaof") {  // setConfigReplicaOfOption()
    if (in.size() != 2) {
      err = "wrong number of arguments";
      return false;
    }
    if (iequals(in[0], "no") && iequals(in[1], "one")) {
      canonical = "no one";
      return true;
    }
    char* ptr = nullptr;
    const long port = std::strtol(in[1].c_str(), &ptr, 10);
    if (port < 0 || port > 65535 || *ptr != '\0') {
      err = "Invalid master port";
      return false;
    }
    canonical = join(in);
    return true;
  }
  if (name == "latency-tracking-info-percentiles") {
    std::vector<std::string> out;
    if (!(in.size() == 1 && in[0].empty())) {
      for (const auto& a : in) {
        double d = 0;
        if (!string2d(a, d)) {
          err = "Invalid latency-tracking-info-percentiles parameters";
          return false;
        }
        if (d > 100.0 || d < 0.0) {
          err = "latency-tracking-info-percentiles parameters should sit between [0.0,100.0]";
          return false;
        }
        out.push_back(trim_double(d));
      }
    }
    canonical = join(out);
    return true;
  }
  err = "unhandled special config " + name;  // a new Special setting after a Redis pin bump
  return false;
}

// ── What PeaDB does with each setting ──

// Settings that configure_encoding_limits() and configure_runtime_settings() (command.cpp) read
// from ServerConfig::raw at startup.
const std::unordered_set<std::string>& raw_applied_settings() {
  static const std::unordered_set<std::string> kSet = {
      "hash-max-listpack-entries", "hash-max-listpack-value", "list-max-listpack-size",
      "set-max-intset-entries", "set-max-listpack-entries", "set-max-listpack-value",
      "zset-max-listpack-entries", "zset-max-listpack-value",
      "min-replicas-to-write", "replica-serve-stale-data", "busy-reply-threshold",
      "replica-ignore-disk-write-errors",
      "lazyfree-lazy-server-del", "stop-writes-on-bgsave-error", "aof-load-truncated",
      "slowlog-log-slower-than", "slowlog-max-len", "latency-monitor-threshold",
      "proto-max-bulk-len", "client-query-buffer-limit", "tcp-keepalive", "lfu-log-factor",
      "maxmemory-eviction-tenacity",
      "enable-protected-configs", "rdbchecksum", "rdb-key-save-delay", "shutdown-on-sigterm",
      "shutdown-on-sigint", "shutdown-timeout"};
  return kSet;
}

const char* const kTlsRefusal =
    "Failed finding TLS support. PeaDB has no TLS (compat/delta.md item 7): terminate TLS in a "
    "proxy in front of PeaDB and remove the tls-port, tls-replication and tls-cluster settings";

const std::unordered_map<std::string, FixedSetting>& fixed_settings() {
  static const std::unordered_map<std::string, FixedSetting> kMap = {
      {"daemonize", {"no", nullptr}},
      {"databases", {"16", nullptr}},
      {"logfile", {"", nullptr}},
      {"pidfile", {"", nullptr}},
      {"syslog-enabled", {"no", nullptr}},
      {"supervised", {"no", nullptr}},
      {"io-threads", {"1", nullptr}},
      {"activedefrag", {"no", nullptr}},
      {"aof-use-rdb-preamble", {"no", nullptr}},
      {"aof-timestamp-enabled", {"no", nullptr}},
      {"auto-aof-rewrite-percentage", {"0", nullptr}},  // no automatic AOF rewrite (item 59)
      {"rdbcompression", {"no", nullptr}},
      {"lfu-decay-time", {"0", nullptr}},  // LFU counters never decay
      {"maxmemory-clients", {"0", nullptr}},
      {"replica-read-only", {"yes", nullptr}},
      {"replica-ignore-maxmemory", {"yes", nullptr}},
      {"tls-port", {"0", kTlsRefusal}},
      {"tls-replication", {"no", kTlsRefusal}},
      {"tls-cluster", {"no", kTlsRefusal}},
      {"aclfile", {"", "PeaDB has no ACL users and cannot load an ACL file (compat/delta.md item "
                       "24): set the default user's password with requirepass instead"}},
      {"masteruser", {"", "PeaDB replicas cannot authenticate to their master as a named ACL "
                          "user (compat/delta.md item 50)"}},
      {"enable-debug-command", {"yes", "PeaDB cannot restrict the DEBUG command yet "
                                       "(compat/delta.md item 61): remove this setting and "
                                       "disable DEBUG with: rename-command DEBUG \"\""}},
      {"replicaof", {"no one", "PeaDB cannot start as a replica from its config file "
                               "(compat/delta.md item 45): start it as a master and send it "
                               "REPLICAOF <host> <port>"}},
  };
  return kMap;
}

// The last line that set a Redis setting PeaDB does not apply. Whether that setting is refused,
// warned about or accepted silently is decided on its final value, once every line and option
// is read, because the last value wins (a later `--replicaof "no one"` undoes an earlier
// replicaof line, as in Redis).
struct NotAppliedLine {
  std::string value;
  int linenum = 0;
  std::string origin;
  std::size_t seq = 0;  // order of the lines, across included files
};

struct LoadState {
  explicit LoadState(ServerConfig& c) : cfg(c) {}
  ServerConfig& cfg;
  // Redis's reading_config_file flag: set while a config text is parsed and cleared when that
  // parse ends, which includes the parse of an included file. So after an include, each later
  // save line replaces the save points instead of adding to them, as in Redis.
  bool reading_config_file = false;
  bool save_loaded = false;  // the first save line replaces the save points
  int include_depth = 0;
  std::size_t seq = 0;
  std::unordered_map<std::string, NotAppliedLine> not_applied;
  NotAppliedLine bad_bind;      // the last bind line, while it names other than one address
  std::string bad_bind_reason;  // empty when the last bind line is fine
  // Redis's command table as rename-command sees it: every Redis 7.2.5 command (SENTINEL exists
  // only in sentinel mode), each current name mapped to the Redis command it stands for.
  bool commands_ready = false;
  std::unordered_map<std::string, std::string> commands;
};

std::string to_upper(std::string s) {
  for (auto& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  return s;
}

std::string line_origin(int linenum, const std::string& line) {
  return "Reading the configuration file, at line " + std::to_string(linenum) + "\n>>> '" +
         line + "'";
}

bool read_config_source(const std::string& path, std::string& text, std::string& error);
bool parse_config_text(LoadState& st, const std::string& text, std::string& error);

// Applies one standard Redis setting whose value has passed Redis's checks.
bool apply_setting(LoadState& st, const RedisConfigSpec& spec, const std::vector<std::string>& args,
                   const std::string& value, int linenum, const std::string& origin,
                   std::string& err) {
  ServerConfig& cfg = st.cfg;
  const std::string name = spec.name;
  if (name == "dir") {
    // setConfigDirOption() changes the working directory at once, so later relative paths in
    // the config (include, logfile, unixsocket) and the data files are relative to it.
    if (::chdir(value.c_str()) != 0) {
      err = std::strerror(errno);
      return false;
    }
    cfg.raw[name] = value;
    return true;
  }
  cfg.raw[name] = value;
  if (raw_applied_settings().count(name) != 0) return true;
  if (name == "port") cfg.port = std::stoi(value);
  else if (name == "maxclients")
    cfg.maxclients = static_cast<int>(std::min<long long>(std::stoll(value), INT_MAX));
  else if (name == "tcp-backlog") cfg.tcp_backlog = std::stoi(value);
  else if (name == "timeout") cfg.timeout = value;
  else if (name == "unixsocket") cfg.unixsocket = value;
  else if (name == "unixsocketperm") cfg.unixsocketperm = std::stoi(value, nullptr, 8);
  else if (name == "loglevel") cfg.log_level = value;
  else if (name == "dbfilename") cfg.dbfilename = value;
  else if (name == "appendonly") cfg.appendonly = value == "yes";
  else if (name == "appendfilename") cfg.appendfilename = value;
  else if (name == "appendfsync") cfg.appendfsync = value;
  else if (name == "requirepass") cfg.requirepass = value;
  else if (name == "masterauth") cfg.masterauth = value;
  else if (name == "protected-mode") cfg.protected_mode = value == "yes";
  else if (name == "enable-module-command") cfg.enable_module_command = value;
  else if (name == "maxmemory") cfg.maxmemory = value;
  else if (name == "maxmemory-policy") cfg.maxmemory_policy = value;
  else if (name == "maxmemory-samples") cfg.maxmemory_samples = value;
  else if (name == "repl-backlog-size") cfg.repl_backlog_size = value;
  else if (name == "notify-keyspace-events") cfg.notify_keyspace_events = value;
  else if (name == "cluster-enabled") cfg.cluster_enabled = value == "yes";
  else if (name == "cluster-announce-ip") cfg.cluster_announce_ip = value;
  else if (name == "cluster-announce-port") cfg.cluster_announce_port = value;
  else if (name == "cluster-announce-bus-port") cfg.cluster_announce_bus_port = value;
  else if (name == "bind") {
    if (args.size() != 1 || value.empty()) {
      st.bad_bind = {value, linenum, origin, st.seq++};
      st.bad_bind_reason = "PeaDB can listen on exactly one bind address, and this line names " +
                           std::to_string(value.empty() ? 0 : args.size()) +
                           " (compat/delta.md item 69)";
    } else {
      st.bad_bind_reason.clear();
      cfg.bind = value;
    }
  } else if (name == "save") {
    // setConfigSaveOption(): `save ""` clears the save points; while a config text is read the
    // first save line replaces them and later ones add to them, and after an include every
    // save line replaces them.
    if (!st.reading_config_file) {
      cfg.save.clear();
    } else if (!st.save_loaded) {
      st.save_loaded = true;
      cfg.save.clear();
    }
    if (args.size() == 1 && args[0].empty()) cfg.save.clear();
    if (!value.empty()) cfg.save += (cfg.save.empty() ? "" : " ") + value;
    cfg.raw[name] = cfg.save;
  } else if (name == "client-output-buffer-limit") {
    if (!value.empty()) {
      cfg.client_output_buffer_limit += (cfg.client_output_buffer_limit.empty() ? "" : " ") + value;
    }
  } else {
    st.not_applied[name] = {value, linenum, origin, st.seq++};
  }
  return true;
}

// One standard Redis setting: arity, value checks, then apply_setting().
bool handle_standard(LoadState& st, const RedisConfigSpec& spec, std::vector<std::string>& argv,
                     int linenum, const std::string& origin, std::string& err) {
  if (!spec.multi_arg && argv.size() != 2) {
    err = "wrong number of arguments";
    return false;
  }
  std::vector<std::string> args(argv.begin() + 1, argv.end());
  // A MULTI_ARG setting given as one non-empty argument is split again, so that
  // `--save "900 1"` and `save "900 1"` mean `save 900 1`.
  if (spec.multi_arg && args.size() == 1 && !args[0].empty()) {
    std::vector<std::string> split;
    if (!split_config_args(args[0], split)) split.clear();
    args = std::move(split);
  }
  std::string value;
  if (!check_redis_config_value(spec, args, value, err)) return false;
  return apply_setting(st, spec, args, value, linenum, origin, err);
}

// rename-command, checked against Redis's command table the way Redis's loader checks it: the
// command must exist, and the new name (unless empty) must not.
bool handle_rename(LoadState& st, const std::vector<std::string>& argv, int linenum,
                   const std::string& origin, std::string& err) {
  if (!st.commands_ready) {
    for (const auto& e : command_metadata()) {
      const std::string n = e.full_name;
      if (n.find('|') == std::string::npos && n != "sentinel") st.commands[to_upper(n)] = n;
    }
    st.commands_ready = true;
  }
  const std::string from = to_upper(argv[1]);
  const std::string to = to_upper(argv[2]);
  const auto it = st.commands.find(from);
  if (it == st.commands.end()) {
    err = "No such command in rename-command";
    return false;
  }
  const std::string redis_name = it->second;
  st.commands.erase(it);
  if (!to.empty() && !st.commands.emplace(to, redis_name).second) {
    err = "Target command name already exists";
    return false;
  }
  st.cfg.rename_commands.push_back({argv[1], argv[2], redis_name, linenum, origin});
  return true;
}

bool parse_config_text(LoadState& st, const std::string& text, std::string& error) {
  // sdssplitlen(config, strlen(config), "\n") — the text ends at the first NUL byte (the file
  // readers below already drop NUL bytes the way Redis's fgets() loop does).
  const std::string config = text.substr(0, text.find('\0'));
  std::vector<std::string> lines;
  std::size_t start = 0;
  while (true) {
    const auto nl = config.find('\n', start);
    lines.push_back(config.substr(start, nl == std::string::npos ? std::string::npos : nl - start));
    if (nl == std::string::npos) break;
    start = nl + 1;
  }
  st.reading_config_file = true;
  for (std::size_t i = 0; i < lines.size(); ++i) {
    const int linenum = static_cast<int>(i) + 1;
    const std::string line = trim_line(lines[i]);
    if (line.empty() || line[0] == '#') continue;
    const std::string origin = line_origin(linenum, line);
    std::string err;
    std::vector<std::string> argv;
    if (!split_config_args(line, argv)) {
      err = "Unbalanced quotes in configuration line";
    } else if (argv.empty()) {
      continue;
    } else {
      argv[0] = to_lower(argv[0]);
      const std::string& name = argv[0];
      const std::size_t argc = argv.size();
      if (const RedisConfigSpec* spec = find_redis_config(name)) {
        handle_standard(st, *spec, argv, linenum, origin, err);
      } else if ((name == "list-max-ziplist-entries" || name == "list-max-ziplist-value" ||
                  name == "lua-replicate-commands") && argc == 2) {
        // Deprecated settings that Redis still accepts and ignores (deprecated_configs[]).
      } else if (name == "include" && argc == 2) {
        if (st.include_depth >= kMaxIncludeDepth) {
          err = "include nesting is deeper than " + std::to_string(kMaxIncludeDepth) +
                " files (an include loop?)";
        } else {
          std::string included;
          if (!read_config_source(argv[1], included, error)) return false;
          ++st.include_depth;
          const bool ok = parse_config_text(st, included, error);
          --st.include_depth;
          if (!ok) return false;
        }
      } else if (name == "rename-command" && argc == 3) {
        handle_rename(st, argv, linenum, origin, err);
      } else if (name == "user" && argc >= 2) {
        err = "PeaDB has no ACL users (compat/delta.md item 24): set the default user's "
              "password with requirepass instead of a user line";
      } else if (name == "loadmodule" && argc >= 2) {
        err = "PeaDB cannot load Redis modules";
      } else if (name.find('.') != std::string::npos) {
        // A module setting: Redis queues it and aborts at startup when no module claims it.
        err = argc < 2 ? "Module config specified without value"
                       : "Module Configuration detected without loadmodule directive or no "
                         "ApplyConfig call: aborting";
      } else if (name == "sentinel") {
        // `sentinel` alone is handled by Redis's main() and does nothing here.
        if (argc != 1) err = "sentinel directive while not in sentinel mode";
      } else {
        err = "Bad directive or wrong number of arguments";
      }
    }
    if (!err.empty()) {
      error = format_config_error(origin, err);
      return false;
    }
  }
  st.reading_config_file = false;
  return true;
}

// After the whole text is read: refuse, warn about or silently accept each Redis setting PeaDB
// does not apply (on its final value), then run the checks Redis makes after its loader.
bool finish_load(LoadState& st, std::string& error) {
  const NotAppliedLine* refused = st.bad_bind_reason.empty() ? nullptr : &st.bad_bind;
  std::string reason = st.bad_bind_reason;
  std::vector<std::pair<std::string, const NotAppliedLine*>> warned;
  const auto& table = fixed_settings();
  for (const auto& [name, line] : st.not_applied) {
    const auto it = table.find(name);
    if (it != table.end() && line.value == it->second.value) continue;
    if (it != table.end() && it->second.refuse != nullptr) {
      if (refused == nullptr || line.seq < refused->seq) {
        refused = &line;
        reason = it->second.refuse;
      }
      continue;
    }
    warned.emplace_back(name, &line);
  }
  if (refused != nullptr) {
    error = format_config_error(refused->origin, reason);
    return false;
  }
  // Redis opens a non-empty logfile once to fail early (end of loadServerConfigFromString()).
  const auto logfile = st.cfg.raw.find("logfile");
  if (logfile != st.cfg.raw.end() && !logfile->second.empty()) {
    FILE* fp = std::fopen(logfile->second.c_str(), "a");
    if (fp == nullptr) {
      error = format_config_error("", std::string("Can't open the log file: ") +
                                          std::strerror(errno));
      return false;
    }
    std::fclose(fp);
  }
  // Redis applies locale-collate with setlocale() at startup and exits when that fails.
  const auto locale = st.cfg.raw.find("locale-collate");
  if (locale != st.cfg.raw.end()) {
    const locale_t loc =
        ::newlocale(LC_COLLATE_MASK, locale->second.c_str(), static_cast<locale_t>(0));
    if (loc == static_cast<locale_t>(0)) {
      error = "Failed to configure LOCALE for invalid locale name.";
      return false;
    }
    ::freelocale(loc);
  }
  std::sort(warned.begin(), warned.end(),
            [](const auto& a, const auto& b) { return a.second->seq < b.second->seq; });
  for (const auto& [name, line] : warned) {
    st.cfg.warnings.push_back("config line " + std::to_string(line->linenum) + ": '" + name +
                              "' is a Redis setting that PeaDB does not support yet, so it has "
                              "no effect (compat/delta.md item 24)");
  }
  return true;
}

// Appends `data` the way Redis's loadServerConfig() reads a file: fgets() of up to
// CONFIG_READ_LEN (1024) bytes or one line, then sdscat(), so a NUL byte drops the rest of that
// read and nothing more.
void append_like_fgets(const std::string& data, std::string& text) {
  constexpr std::size_t kConfigReadLen = 1024;
  std::size_t pos = 0;
  while (pos < data.size()) {
    std::size_t end = pos;
    while (end < data.size() && end - pos < kConfigReadLen) {
      if (data[end++] == '\n') break;
    }
    const auto nul = data.find('\0', pos);
    text.append(data, pos, (nul != std::string::npos && nul < end ? nul : end) - pos);
    pos = end;
  }
}

bool read_one_file(const std::string& path, std::string& text, std::string& error) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    error = "Fatal error, can't open config file '" + path + "': " + std::strerror(errno);
    return false;
  }
  const std::string data{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
  append_like_fgets(data, text);
  return true;
}

// The file part of loadServerConfig(): a name with a wildcard is a glob, and a glob that matches
// nothing is skipped (so an empty conf.d directory can be included).
bool read_config_source(const std::string& path, std::string& text, std::string& error) {
  if (path.find_first_of("*?[") == std::string::npos) return read_one_file(path, text, error);
  glob_t globbuf{};
  if (::glob(path.c_str(), 0, nullptr, &globbuf) == 0) {
    for (std::size_t i = 0; i < globbuf.gl_pathc; ++i) {
      if (!read_one_file(globbuf.gl_pathv[i], text, error)) {
        ::globfree(&globbuf);
        return false;
      }
    }
    ::globfree(&globbuf);
  }
  return true;
}

}  // namespace

const RedisConfigSpec* find_redis_config(const std::string& name) {
  static const auto* index = [] {
    auto* m = new std::unordered_map<std::string, const RedisConfigSpec*>();
    for (const auto& spec : kRedisConfigSpecs) {
      (*m)[spec.name] = &spec;
      if (spec.alias != nullptr) (*m)[spec.alias] = &spec;
    }
    return m;
  }();
  const auto it = index->find(to_lower(name));
  return it == index->end() ? nullptr : it->second;
}

std::vector<std::string> config_set_args(const RedisConfigSpec& spec, const std::string& value) {
  if (!spec.multi_arg) return {value};
  // sdssplitlen(value, len, " ", 1): every single space separates, so "a  b" has an empty
  // middle argument, and an empty value has no arguments at all.
  std::vector<std::string> out;
  if (value.empty()) return out;
  std::size_t start = 0;
  while (true) {
    const auto sp = value.find(' ', start);
    out.push_back(value.substr(start, sp == std::string::npos ? std::string::npos : sp - start));
    if (sp == std::string::npos) break;
    start = sp + 1;
  }
  return out;
}

bool check_redis_config_value(const RedisConfigSpec& spec, const std::vector<std::string>& args,
                              std::string& value, std::string& err) {
  if (!spec.multi_arg && args.size() != 1) {
    err = "wrong number of arguments";
    return false;
  }
  switch (spec.type) {
    case RedisConfigType::Bool:
      return check_bool(spec, args[0], value, err);
    case RedisConfigType::Enum:
      return check_enum(spec, args, value, err);
    case RedisConfigType::Numeric: {
      long long ll = 0;
      if (!check_numeric(spec, args[0], ll, err)) return false;
      // numericConfigGet(): a percentage as N%, an octal setting in base 8, else decimal.
      if ((spec.numeric_flags & kNumPercent) != 0 && ll < 0) {
        value = std::to_string(-ll) + "%";
      } else if ((spec.numeric_flags & kNumOctal) != 0) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%llo", static_cast<unsigned long long>(ll));
        value = buf;
      } else {
        value = spec.numeric_unsigned ? std::to_string(static_cast<unsigned long long>(ll))
                                      : std::to_string(ll);
      }
      return true;
    }
    case RedisConfigType::String:
      if (!check_string(spec, args[0], err)) return false;
      value = args[0];
      return true;
    case RedisConfigType::Special:
      return check_special(spec.name, args, value, err);
  }
  return false;
}

const FixedSetting* peadb_fixed_setting(const std::string& name) {
  const auto& table = fixed_settings();
  const auto it = table.find(name);
  return it == table.end() ? nullptr : &it->second;
}

bool split_config_args(const std::string& line, std::vector<std::string>& out) {
  out.clear();
  const char* p = line.c_str();
  while (true) {
    while (*p && std::isspace(static_cast<unsigned char>(*p))) ++p;
    if (!*p) return true;
    bool inq = false;   // inside "double quotes"
    bool insq = false;  // inside 'single quotes'
    bool done = false;
    std::string current;
    while (!done) {
      if (inq) {
        if (*p == '\\' && *(p + 1) == 'x' && is_hex_digit(*(p + 2)) && is_hex_digit(*(p + 3))) {
          current.push_back(static_cast<char>(hex_digit_to_int(*(p + 2)) * 16 +
                                              hex_digit_to_int(*(p + 3))));
          p += 3;
        } else if (*p == '\\' && *(p + 1)) {
          ++p;
          switch (*p) {
            case 'n': current.push_back('\n'); break;
            case 'r': current.push_back('\r'); break;
            case 't': current.push_back('\t'); break;
            case 'b': current.push_back('\b'); break;
            case 'a': current.push_back('\a'); break;
            default: current.push_back(*p); break;
          }
        } else if (*p == '"') {
          // A closing quote must be followed by a space or nothing at all.
          if (*(p + 1) && !std::isspace(static_cast<unsigned char>(*(p + 1)))) {
            out.clear();
            return false;
          }
          done = true;
        } else if (!*p) {
          out.clear();  // unterminated quotes
          return false;
        } else {
          current.push_back(*p);
        }
      } else if (insq) {
        if (*p == '\\' && *(p + 1) == '\'') {
          ++p;
          current.push_back('\'');
        } else if (*p == '\'') {
          if (*(p + 1) && !std::isspace(static_cast<unsigned char>(*(p + 1)))) {
            out.clear();
            return false;
          }
          done = true;
        } else if (!*p) {
          out.clear();
          return false;
        } else {
          current.push_back(*p);
        }
      } else {
        switch (*p) {
          case ' ': case '\n': case '\r': case '\t': case '\0': done = true; break;
          case '"': inq = true; break;
          case '\'': insq = true; break;
          default: current.push_back(*p); break;
        }
      }
      if (*p) ++p;
    }
    out.push_back(std::move(current));
  }
}

std::string config_options_from_argv(const std::vector<std::string>& argv, std::size_t first,
                                     bool& from_stdin) {
  // The option loop of Redis's main() (server.c).
  std::string options;
  bool handled_last_config_arg = true;
  const std::size_t argc = argv.size();
  for (std::size_t j = first; j < argc; ++j) {
    const std::string& arg = argv[j];
    if (arg == "-" && (j == 1 || j == argc - 1)) {
      from_stdin = true;
    } else if (handled_last_config_arg && arg.size() >= 2 && arg[0] == '-' && arg[1] == '-') {
      if (!options.empty()) options += "\n";
      options += arg.substr(2) + " ";
      std::vector<std::string> split;
      split_config_args(arg, split);
      if (split.size() == 1) {
        // Only an option name, like --port: its value is the next argument.
        handled_last_config_arg = false;
        const bool next_is_option = j != argc - 1 && argv[j + 1].size() >= 2 &&
                                    argv[j + 1][0] == '-' && argv[j + 1][1] == '-';
        if (next_is_option && iequals(arg, "--save")) {
          // `--save --port 6380` means `--save "" --port 6380`, as before Redis 7.0.1.
          options += "\"\"";
          handled_last_config_arg = true;
        } else if (j == argc - 1 && iequals(arg, "--save")) {
          options += "\"\"";
        } else if (next_is_option && iequals(arg, "--sentinel")) {
          handled_last_config_arg = true;  // a pseudo option with no value
        }
      } else {
        handled_last_config_arg = true;  // name and value in one argument: "--port 6380"
      }
    } else {
      options += quote_arg(arg) + " ";
      handled_last_config_arg = true;
    }
  }
  return options;
}

bool load_config_string(const std::string& text, ServerConfig& cfg, std::string& error) {
  LoadState st(cfg);
  return parse_config_text(st, text, error) && finish_load(st, error);
}

bool load_server_config(const std::string& path, bool from_stdin, const std::string& options,
                        ServerConfig& cfg, std::string& error) {
  std::string text;
  if (!path.empty() && !read_config_source(path, text, error)) return false;
  if (from_stdin) {
    const std::string data{std::istreambuf_iterator<char>(std::cin),
                           std::istreambuf_iterator<char>()};
    append_like_fgets(data, text);
  }
  if (!options.empty()) {
    text += "\n";
    text += options;
  }
  return load_config_string(text, cfg, error);
}

std::string format_config_error(const std::string& origin, const std::string& reason) {
  std::string out = "\n*** FATAL CONFIG FILE ERROR (PeaDB " PEADB_VERSION ") ***\n";
  if (!origin.empty()) out += origin + "\n";
  return out + reason + "\n";
}

}  // namespace peadb
