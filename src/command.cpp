#include "command.hpp"
#include "server_context.hpp"

#include "datastore.hpp"
#include "errors.hpp"
#include "lua_engine.hpp"
#include "protocol.hpp"
#include "rdb.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <functional>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <iomanip>
#include <filesystem>
#include <fstream>
#include <thread>
#include <cstdlib>
#include <string_view>
#include <array>
#include <set>
#include <memory>
#include <limits>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <dlfcn.h>
#include <fnmatch.h>
#include <netdb.h>

#include "logger.hpp"

namespace peadb {

// Forward declarations for functions defined after the anonymous namespace
void register_key_waiter(const std::string& key, int fd);
void unregister_key_waiter(int fd);
void notify_key_written(const std::string& key, int db_index);
bool replica_do_fullsync(const std::string& host, int port);

namespace {

enum CmdFlag : std::uint32_t {
  kFlagWrite    = 1u << 0,
  kFlagReadonly = 1u << 1,
  kFlagFast     = 1u << 2,
  kFlagAdmin    = 1u << 3,
  kFlagNoScript = 1u << 4,
  kFlagPubSub   = 1u << 5,
  kFlagLoading  = 1u << 6,
  kFlagStale    = 1u << 7,
  kFlagNoAuth   = 1u << 8,
};

struct CommandSpec {
  std::string name;
  int arity;
  std::vector<std::string> flags;
  int first_key;
  int last_key;
  int key_step;
  std::function<std::string(const std::vector<std::string>&, SessionState&, bool&)> handler;
  std::uint32_t flag_bits = 0;
  bool is_write() const { return flag_bits & kFlagWrite; }
};

std::unordered_map<std::string, std::string> g_script_cache;
std::filesystem::path g_snapshot_path = "dump.rdb";
std::filesystem::path g_aof_path = "appendonly.aof";
bool g_appendonly = false;
// thread_local: each thread (event loop, REPLICAOF streaming) maintains its own
// replay flag so setting it in one thread never silently suppresses AOF writes
// or bypasses READONLY enforcement in the other.
thread_local bool g_replaying_aof = false;
int g_aof_fd = -1;
std::string g_appendfsync = "everysec";
bool g_aof_write_error = false;
std::int64_t g_last_aof_fsync_ms = 0;
std::atomic<pid_t> g_bgsave_child_pid{0};
std::atomic<bool> g_bgsave_in_progress{false};
std::int64_t g_bgsave_start_time = 0;
std::string g_last_bgsave_status = "ok";
std::atomic<pid_t> g_bgrewrite_child_pid{0};
std::atomic<bool> g_bgrewrite_in_progress{false};
std::string g_aof_rewrite_buf;  // Parent buffers commands written during child rewrite
bool g_active_expire = true;
std::int64_t g_last_save_time = 0;
std::string g_replication_role = "master";
std::string g_replication_master_host = "";
int g_replication_master_port = 0;
int g_server_port = 6379;
std::string g_master_replid = "0000000000000000000000000000000000000001";
std::int64_t g_master_repl_offset = 0;
std::int64_t g_config_maxmemory = 0;
std::int64_t g_config_min_replicas_to_write = 0;
bool g_in_lua_script = false;
bool g_config_replica_serve_stale_data = true;
// 1MB circular replication backlog (replaces unbounded g_replication_events vector)
struct ReplBacklog {
    static constexpr std::size_t kSize = 1 << 20; // 1MB
    std::vector<char> buf;
    std::int64_t total = 0; // total bytes ever appended

    ReplBacklog() : buf(kSize, 0) {}

    std::int64_t base() const {
        return total > static_cast<std::int64_t>(kSize)
               ? total - static_cast<std::int64_t>(kSize) : 0;
    }

    void append(const std::string& data) {
        for (char c : data) {
            buf[static_cast<std::size_t>(total++ % kSize)] = c;
        }
    }

    // Fill out with bytes [from_byte, total). Returns false if from_byte < base().
    bool get_slice(std::int64_t from_byte, std::string& out) const {
        if (from_byte < base() || from_byte > total) return false;
        const std::size_t len = static_cast<std::size_t>(total - from_byte);
        out.resize(len);
        if (len == 0) return true;
        const std::size_t start = static_cast<std::size_t>(from_byte % kSize);
        if (start + len <= kSize) {
            std::memcpy(out.data(), buf.data() + start, len);
        } else {
            const std::size_t first = kSize - start;
            std::memcpy(out.data(), buf.data() + start, first);
            std::memcpy(out.data() + first, buf.data(), len - first);
        }
        return true;
    }
};
ReplBacklog g_repl_backlog;
// Append a RESP-encoded command to the replication backlog and advance the master offset
inline void backlog_append(const std::string& data) {
    g_repl_backlog.append(data);
    g_master_repl_offset = g_repl_backlog.total;
}
std::vector<std::string> g_exec_replication_events;
bool g_capture_exec_replication = false;
int g_exec_last_repl_db = -1;
int g_exec_write_event_count = 0;
bool g_executing_exec = false;
int g_last_repl_db = 0;
bool g_replica_stale = false;
std::atomic<bool> g_loading_replication{false};
bool g_script_allow_oom = false;
bool g_script_inner_error_seen = false;
enum class SlotRoute { Owned, Moved, Ask };
std::array<SlotRoute, 16384> g_slot_routes {};
std::string g_cluster_redirect_addr = "127.0.0.1:7000";
std::set<std::string> g_cluster_peers;
struct LoadedModule {
  void* handle = nullptr;
  std::string path;
};
std::unordered_map<std::string, LoadedModule> g_loaded_modules;
using ModuleCommandFn = int (*)(void*, void**, int);
std::unordered_map<std::string, ModuleCommandFn> g_module_commands;
std::unordered_map<std::string, std::string> g_module_command_owner;
std::string g_current_loading_module;
struct ModuleKeyHandle {
  std::string key;
};
std::vector<std::unique_ptr<ModuleKeyHandle>> g_module_keys;
std::string g_module_dma_buf;
struct ModuleCallCtx {
  std::string reply;
  bool replied = false;
};
struct FunctionDef {
  std::string library;
  std::string body;
  bool no_writes = false;
};
struct CmdStats {
  std::int64_t calls = 0;
  std::int64_t rejected_calls = 0;
  std::int64_t failed_calls = 0;
};
bool parse_i64(const std::string& s, std::int64_t& out);
std::unordered_map<std::string, FunctionDef> g_functions;
std::unordered_map<std::string, std::vector<std::string>> g_function_libraries;
bool g_script_readonly_context = false;
RespVersion g_script_client_resp_version = RespVersion::Resp2;
SessionState* g_script_current_session = nullptr;
std::uint64_t g_script_prng_state = 0x12345678ULL;
std::unordered_map<std::string, CmdStats> g_cmdstats;
std::unordered_map<std::string, std::int64_t> g_errorstats;
std::int64_t g_total_error_replies = 0;

} // end anonymous namespace

// ── Script busy / kill state (external linkage for lua_engine.cpp) ──
std::int64_t g_config_lua_time_limit = 5000;
std::atomic<bool> g_script_busy{false};
std::atomic<bool> g_script_kill_requested{false};
SessionState* g_busy_script_session = nullptr;

// ── Server-wide stats (defined here, declared extern in command.hpp) ──
std::atomic<int> g_connected_clients{0};
std::atomic<std::int64_t> g_total_connections_received{0};
std::atomic<std::int64_t> g_total_commands_processed{0};
std::atomic<std::int64_t> g_server_start_time{0};
std::atomic<int> g_connected_replicas{0};
std::atomic<int> g_runtime_max_clients{10000};
std::function<int(std::int64_t)> g_count_synced_replicas;
std::atomic<std::uint64_t> g_next_client_id{1};
std::function<std::string()> g_client_list_callback;
std::function<bool(int fd, const std::string& data)> g_pubsub_send;

// ── Pub/Sub registry ──
namespace {
// channel -> set of subscriber fds
std::unordered_map<std::string, std::unordered_set<int>> g_channel_subscribers;
// pattern -> set of subscriber fds
std::unordered_map<std::string, std::unordered_set<int>> g_pattern_subscribers;
// fd -> set of subscribed channels (for cleanup on disconnect)
std::unordered_map<int, std::unordered_set<std::string>> g_fd_channels;
// fd -> set of subscribed patterns
std::unordered_map<int, std::unordered_set<std::string>> g_fd_patterns;
} // anonymous

void pubsub_subscribe(int fd, const std::string& channel) {
  g_channel_subscribers[channel].insert(fd);
  g_fd_channels[fd].insert(channel);
}
void pubsub_unsubscribe(int fd, const std::string& channel) {
  auto it = g_channel_subscribers.find(channel);
  if (it != g_channel_subscribers.end()) {
    it->second.erase(fd);
    if (it->second.empty()) g_channel_subscribers.erase(it);
  }
  auto fit = g_fd_channels.find(fd);
  if (fit != g_fd_channels.end()) {
    fit->second.erase(channel);
    if (fit->second.empty()) g_fd_channels.erase(fit);
  }
}
void pubsub_psubscribe(int fd, const std::string& pattern) {
  g_pattern_subscribers[pattern].insert(fd);
  g_fd_patterns[fd].insert(pattern);
}
void pubsub_punsubscribe(int fd, const std::string& pattern) {
  auto it = g_pattern_subscribers.find(pattern);
  if (it != g_pattern_subscribers.end()) {
    it->second.erase(fd);
    if (it->second.empty()) g_pattern_subscribers.erase(it);
  }
  auto fit = g_fd_patterns.find(fd);
  if (fit != g_fd_patterns.end()) {
    fit->second.erase(pattern);
    if (fit->second.empty()) g_fd_patterns.erase(fit);
  }
}
void pubsub_unsubscribe_all(int fd) {
  auto cit = g_fd_channels.find(fd);
  if (cit != g_fd_channels.end()) {
    for (const auto& ch : cit->second) {
      auto sit = g_channel_subscribers.find(ch);
      if (sit != g_channel_subscribers.end()) {
        sit->second.erase(fd);
        if (sit->second.empty()) g_channel_subscribers.erase(sit);
      }
    }
    g_fd_channels.erase(cit);
  }
  auto pit = g_fd_patterns.find(fd);
  if (pit != g_fd_patterns.end()) {
    for (const auto& pat : pit->second) {
      auto sit = g_pattern_subscribers.find(pat);
      if (sit != g_pattern_subscribers.end()) {
        sit->second.erase(fd);
        if (sit->second.empty()) g_pattern_subscribers.erase(sit);
      }
    }
    g_fd_patterns.erase(pit);
  }
}

namespace {
std::string encode_pubsub_message(const std::string& type, const std::string& channel, const std::string& data) {
  return "*3\r\n$" + std::to_string(type.size()) + "\r\n" + type + "\r\n"
       + "$" + std::to_string(channel.size()) + "\r\n" + channel + "\r\n"
       + "$" + std::to_string(data.size()) + "\r\n" + data + "\r\n";
}
std::string encode_pmessage(const std::string& pattern, const std::string& channel, const std::string& data) {
  return std::string("*4\r\n$8\r\npmessage\r\n")
       + "$" + std::to_string(pattern.size()) + "\r\n" + pattern + "\r\n"
       + "$" + std::to_string(channel.size()) + "\r\n" + channel + "\r\n"
       + "$" + std::to_string(data.size()) + "\r\n" + data + "\r\n";
}
bool pattern_matches(const std::string& pattern, const std::string& str) {
  return fnmatch(pattern.c_str(), str.c_str(), 0) == 0;
}
} // anonymous

std::int64_t pubsub_publish(const std::string& channel, const std::string& message) {
  std::int64_t receivers = 0;
  if (!g_pubsub_send) return 0;

  // Exact channel subscribers
  auto it = g_channel_subscribers.find(channel);
  if (it != g_channel_subscribers.end()) {
    std::string msg = encode_pubsub_message("message", channel, message);
    for (int fd : it->second) {
      if (g_pubsub_send(fd, msg)) ++receivers;
    }
  }
  // Pattern subscribers
  for (const auto& [pat, fds] : g_pattern_subscribers) {
    if (pattern_matches(pat, channel)) {
      std::string msg = encode_pmessage(pat, channel, message);
      for (int fd : fds) {
        if (g_pubsub_send(fd, msg)) ++receivers;
      }
    }
  }
  return receivers;
}

namespace {

std::string upper(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return s;
}

std::string lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

bool parse_eval_set_keys1_literal(const std::string& script, std::string& literal) {
  constexpr const char* kPrefix1 = "redis.call('set', KEYS[1], '";
  constexpr const char* kPrefix2 = "redis.call('SET', KEYS[1], '";
  constexpr const char* kSuffix = "')";
  auto parse_with_prefix = [&](const char* pfx) -> bool {
    const std::string prefix(pfx);
    if (script.rfind(prefix, 0) != 0) return false;
    if (script.size() < prefix.size() + 2) return false;
    if (script.substr(script.size() - 2) != kSuffix) return false;
    literal = script.substr(prefix.size(), script.size() - prefix.size() - 2);
    return true;
  };
  return parse_with_prefix(kPrefix1) || parse_with_prefix(kPrefix2);
}

bool parse_function_load_payload(const std::string& payload, std::string& library, std::string& func_name,
                                 std::string& body, bool& no_writes) {
  library.clear();
  func_name.clear();
  body.clear();
  no_writes = false;
  const std::string header = "#!lua name=";
  if (payload.rfind(header, 0) != 0) return false;
  const auto nl = payload.find('\n');
  if (nl == std::string::npos || nl <= header.size()) return false;
  library = payload.substr(header.size(), nl - header.size());
  const auto script = payload.substr(nl + 1);
  no_writes = script.find("no-writes") != std::string::npos;
  auto trim = [](std::string& s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.erase(s.begin());
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.pop_back();
  };

  const auto fn1 = script.find("redis.register_function('");
  if (fn1 != std::string::npos) {
    const auto n1s = fn1 + std::string("redis.register_function('").size();
    const auto n1e = script.find('\'', n1s);
    if (n1e == std::string::npos) return false;
    func_name = script.substr(n1s, n1e - n1s);
    std::size_t b1sig = script.find("function(KEYS, ARGV)", n1e);
    std::size_t sig_len = std::string("function(KEYS, ARGV)").size();
    if (b1sig == std::string::npos) {
      b1sig = script.find("function()", n1e);
      sig_len = std::string("function()").size();
    }
    if (b1sig == std::string::npos) return false;
    const auto b1nl = script.find('\n', b1sig);
    if (b1nl != std::string::npos) {
      const auto b1e = script.rfind("\nend)");
      if (b1e == std::string::npos || b1e <= b1nl) return false;
      body = script.substr(b1nl + 1, b1e - (b1nl + 1));
    } else {
      const auto b1e = script.rfind("end)");
      if (b1e == std::string::npos || b1e <= b1sig + sig_len) return false;
      body = script.substr(b1sig + sig_len, b1e - (b1sig + sig_len));
    }
    trim(body);
    return true;
  }

  const auto fn2 = script.find("function_name='");
  if (fn2 != std::string::npos) {
    const auto n2s = fn2 + std::string("function_name='").size();
    const auto n2e = script.find('\'', n2s);
    if (n2e == std::string::npos) return false;
    func_name = script.substr(n2s, n2e - n2s);
    const auto b2sig = script.find("callback=function(KEYS, ARGV)", n2e);
    if (b2sig == std::string::npos) return false;
    const auto b2s = script.find('\n', b2sig);
    const auto b2e = script.rfind("\nend");
    if (b2s == std::string::npos || b2e == std::string::npos || b2e <= b2s) return false;
    body = script.substr(b2s + 1, b2e - (b2s + 1));
    trim(body);
    return true;
  }
  return false;
}

std::string normalize_script(std::string s) {
  for (char& c : s) {
    if (c == '\n' || c == '\r' || c == '\t') c = ' ';
  }
  std::string out;
  out.reserve(s.size());
  bool prev_space = false;
  for (char c : s) {
    const bool is_space = std::isspace(static_cast<unsigned char>(c));
    if (is_space) {
      if (!prev_space) out.push_back(' ');
    } else {
      out.push_back(c);
    }
    prev_space = is_space;
  }
  while (!out.empty() && out.front() == ' ') out.erase(out.begin());
  while (!out.empty() && out.back() == ' ') out.pop_back();
  return out;
}

bool arity_ok(std::size_t argc, int arity) {
  if (arity >= 0) return argc == static_cast<std::size_t>(arity);
  return argc >= static_cast<std::size_t>(-arity);
}

std::int64_t now_ms() {
  return DataStore::now_ms();
}

std::string encode_simple(const std::string& value) { return "+" + value + "\r\n"; }
std::string encode_error(const std::string& value) { return "-ERR " + value + "\r\n"; }
std::string encode_integer(long long value) { return ":" + std::to_string(value) + "\r\n"; }
std::string encode_bulk(const std::string& value) { return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n"; }

// Format a double score as Redis does: up to 17 significant digits, no trailing zeros.
std::string format_score(double d) {
  char buf[64];
  snprintf(buf, sizeof(buf), "%.17g", d);
  return buf;
}
std::string encode_null(RespVersion version) { return version == RespVersion::Resp3 ? "_\r\n" : "$-1\r\n"; }
std::string encode_null_array() { return "*-1\r\n"; }

std::string encode_array(const std::vector<std::string>& encoded_items) {
  std::string out = "*" + std::to_string(encoded_items.size()) + "\r\n";
  for (const auto& item : encoded_items) out += item;
  return out;
}

bool is_error_reply(const std::string& r) { return !r.empty() && r[0] == '-'; }

std::string error_code_from_reply(const std::string& r) {
  if (!is_error_reply(r)) return "";
  const auto sp = r.find(' ');
  if (sp == std::string::npos || sp <= 1) return "ERR";
  return r.substr(1, sp - 1);
}

void record_error_stat(const std::string& code) {
  if (code.empty()) return;
  ++g_total_error_replies;
  ++g_errorstats[upper(code)];
}

void record_cmd_call(const std::string& cmd) {
  ++g_cmdstats[lower(cmd)].calls;
}

void record_cmd_rejected(const std::string& cmd) {
  ++g_cmdstats[lower(cmd)].rejected_calls;
}

void record_cmd_failed(const std::string& cmd) {
  ++g_cmdstats[lower(cmd)].failed_calls;
}

std::string busy_script_error_reply() {
  return "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n";
}

std::string masterdown_stale_reply() {
  return "-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n";
}

std::string encode_command_resp(const std::vector<std::string>& args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto& a : args) {
    out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
  }
  return out;
}

std::size_t encode_command_resp_size(const std::vector<std::string>& args) {
  // *<count>\r\n + for each: $<len>\r\n<data>\r\n
  auto digits = [](std::size_t n) -> std::size_t {
    if (n == 0) return 1;
    std::size_t d = 0;
    while (n > 0) { ++d; n /= 10; }
    return d;
  };
  std::size_t total = 1 + digits(args.size()) + 2; // *<count>\r\n
  for (const auto& a : args) {
    total += 1 + digits(a.size()) + 2 + a.size() + 2; // $<len>\r\n<data>\r\n
  }
  return total;
}

void append_replication_event(const std::vector<std::string>& args, const SessionState& session,
                              const std::string* reply_ptr = nullptr) {
  if (args.empty()) return;
  if (g_replaying_aof) return;  // Suppress replication broadcast while replaying from master or AOF
  const auto cmd = upper(args[0]);
  std::vector<std::string> out = args;
  bool emit = true;

  auto key_exists = [&](const std::string& key) {
    return store().type_of(key) != "none";
  };
  auto abs_ttl = [&](const std::string& key) {
    return store().pexpiretime(key);
  };

  if (cmd == "GETEX") {
    if (args.size() == 2) {
      emit = false;
    } else if (upper(args[2]) == "PERSIST") {
      if (key_exists(args[1])) out = {"PERSIST", args[1]};
      else emit = false;
    } else {
      const auto abs = abs_ttl(args[1]);
      if (abs == -2) out = {"DEL", args[1]};
      else if (abs >= 0) out = {"PEXPIREAT", args[1], std::to_string(abs)};
      else emit = false;
    }
  } else if (cmd == "GETDEL" && args.size() == 2) {
    out = {"DEL", args[1]};
  } else if (cmd == "SPOP" && args.size() >= 2 && reply_ptr != nullptr) {
    std::vector<std::string> removed;
    const auto& r = *reply_ptr;
    auto parse_len = [&](std::size_t start, std::size_t& pos, std::int64_t& len) -> bool {
      const auto eol = r.find("\r\n", start);
      if (eol == std::string::npos) return false;
      try {
        len = std::stoll(r.substr(start, eol - start));
      } catch (...) {
        return false;
      }
      pos = eol + 2;
      return true;
    };

    if (!r.empty() && r[0] == '$') {
      std::size_t pos = 0;
      std::int64_t len = -1;
      if (parse_len(1, pos, len) && len >= 0) {
        const std::size_t need = static_cast<std::size_t>(len);
        if (pos + need + 2 <= r.size()) {
          removed.push_back(r.substr(pos, need));
        }
      }
    } else if (!r.empty() && r[0] == '*') {
      std::size_t pos = 0;
      std::int64_t count = -1;
      if (parse_len(1, pos, count) && count > 0) {
        for (std::int64_t i = 0; i < count; ++i) {
          if (pos >= r.size() || r[pos] != '$') {
            removed.clear();
            break;
          }
          ++pos;
          std::int64_t len = -1;
          if (!parse_len(pos, pos, len) || len < 0) {
            removed.clear();
            break;
          }
          const std::size_t need = static_cast<std::size_t>(len);
          if (pos + need + 2 > r.size()) {
            removed.clear();
            break;
          }
          removed.push_back(r.substr(pos, need));
          pos += need + 2;
        }
      }
    }

    if (removed.empty()) {
      emit = false;
    } else {
      out = {"SREM", args[1]};
      out.insert(out.end(), removed.begin(), removed.end());
    }
  } else if ((cmd == "DEL" || cmd == "UNLINK") && reply_ptr != nullptr) {
    if (reply_ptr->empty() || (*reply_ptr)[0] != ':') emit = false;
    else {
      try {
        const auto n = std::stoll(reply_ptr->substr(1));
        if (n <= 0) emit = false;
      } catch (...) {
        emit = false;
      }
    }
  } else if (cmd == "SET" && args.size() >= 4) {
    std::string exp_opt;
    for (std::size_t i = 3; i < args.size(); ++i) {
      const auto o = upper(args[i]);
      if (o == "EX" || o == "PX" || o == "EXAT" || o == "PXAT") { exp_opt = args[i]; break; }
    }
    if (!exp_opt.empty()) {
      const auto abs = abs_ttl(args[1]);
      if (abs >= 0) out = {"SET", args[1], args[2], exp_opt == "pxat" ? "pxat" : "PXAT", std::to_string(abs)};
    }
  } else if ((cmd == "SETEX" || cmd == "PSETEX") && args.size() == 4) {
    const auto abs = abs_ttl(args[1]);
    if (abs >= 0) out = {"SET", args[1], args[3], "PXAT", std::to_string(abs)};
  } else if ((cmd == "EXPIRE" || cmd == "PEXPIRE" || cmd == "EXPIREAT" || cmd == "PEXPIREAT") && args.size() >= 3) {
    const auto abs = abs_ttl(args[1]);
    if (abs == -2) out = {"DEL", args[1]};
    else if (abs >= 0) out = {"PEXPIREAT", args[1], std::to_string(abs)};
    else emit = false;
  } else if (cmd == "RESTORE" && args.size() >= 4) {
    const auto abs = abs_ttl(args[1]);
    if (abs >= 0) {
      std::string absttl = "ABSTTL";
      for (std::size_t i = 4; i < args.size(); ++i) {
        if (upper(args[i]) == "ABSTTL") { absttl = args[i]; break; }
      }
      out = {"RESTORE", args[1], std::to_string(abs), "{" + args[3] + "}", absttl};
    }
  } else if (cmd == "SCRIPT") {
    emit = false;
  } else if (cmd == "XREADGROUP") {
    emit = false;
  } else if (cmd == "EVAL" || cmd == "EVALSHA" || cmd == "EVAL_RO" || cmd == "FCALL" || cmd == "FCALL_RO") {
    // With real Lua engine, individual commands from within the script
    // are dispatched through handle_command which handles replication.
    // The EVAL/FCALL itself should not emit replication events.
    emit = false;
  } else if (cmd == "INCRBYFLOAT" && args.size() >= 3) {
    // Rewrite INCRBYFLOAT to SET key newvalue KEEPTTL for replication
    auto v = store().get(args[1]);
    if (v.has_value()) {
      out = {"SET", args[1], *v, "KEEPTTL"};
    }
  }

  if (!emit || out.empty()) return;
  if (g_capture_exec_replication) {
    if (g_exec_last_repl_db != session.db_index) {
      g_exec_replication_events.push_back(encode_command_resp({"SELECT", std::to_string(session.db_index)}));
      g_exec_last_repl_db = session.db_index;
    }
    g_exec_replication_events.push_back(encode_command_resp(out));
    ++g_exec_write_event_count;
    return;
  }
  if (session.db_index != g_last_repl_db) {
    backlog_append(encode_command_resp({"SELECT", std::to_string(session.db_index)}));
    g_last_repl_db = session.db_index;
  }
  backlog_append(encode_command_resp(out));
}

void append_lazy_expire_dels(const SessionState& session) {
  const auto expired = store().collect_expired_keys();
  for (const auto& key : expired) {
    append_replication_event({"DEL", key}, session);
  }
}

void open_aof_fd() {
  if (g_aof_fd >= 0) { ::close(g_aof_fd); g_aof_fd = -1; }
  g_aof_fd = ::open(g_aof_path.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
}

void append_aof(const std::vector<std::string>& args) {
  if (!g_appendonly || g_replaying_aof) return;
  if (g_aof_fd < 0) open_aof_fd();
  if (g_aof_fd < 0) { g_aof_write_error = true; return; }
  const auto payload = encode_command_resp(args);
  const ssize_t written = ::write(g_aof_fd, payload.data(), payload.size());
  if (written < 0 || static_cast<std::size_t>(written) != payload.size()) {
    g_aof_write_error = true;
    return;
  }
  if (g_appendfsync == "always") ::fsync(g_aof_fd);
  // Buffer for BGREWRITEAOF diff: commands written while child is rewriting
  // are collected here and appended to the new AOF when the child finishes.
  if (g_bgrewrite_in_progress.load(std::memory_order_relaxed)) {
    // Cap at 256 MiB to bound RSS during a long rewrite on a write-heavy master.
    constexpr std::size_t kMaxRewriteBuf = 256ULL * 1024 * 1024;
    if (g_aof_rewrite_buf.size() < kMaxRewriteBuf) {
      g_aof_rewrite_buf.append(payload);
    }
  }
}

bool rewrite_aof_file(std::string& err) {
  err.clear();
  const std::string tmp_path = g_aof_path.string() + ".tmp";
  const int tmp_fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (tmp_fd < 0) { err = "cannot open AOF tmp for rewrite"; return false; }
  const auto cmds = store().export_aof_commands();
  for (const auto& cmd : cmds) {
    const auto payload = encode_command_resp(cmd);
    if (::write(tmp_fd, payload.data(), payload.size()) < 0) {
      ::close(tmp_fd); ::unlink(tmp_path.c_str());
      err = "AOF rewrite write error"; return false;
    }
  }
  ::fsync(tmp_fd);
  ::close(tmp_fd);
  if (::rename(tmp_path.c_str(), g_aof_path.c_str()) != 0) {
    ::unlink(tmp_path.c_str());
    err = "AOF rewrite rename failed"; return false;
  }
  // Reopen persistent fd to point at the new file
  open_aof_fd();
  return true;
}

void try_send_cluster_meetback(const std::string& host, int port) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return;
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &sa.sin_addr) != 1) { close(fd); return; }
  if (connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) { close(fd); return; }
  std::vector<std::string> args = {"CLUSTER", "MEETBACK", "127.0.0.1", std::to_string(g_server_port)};
  const auto payload = encode_command_resp(args);
  std::size_t sent = 0;
  while (sent < payload.size()) {
    const auto rc = write(fd, payload.data() + sent, payload.size() - sent);
    if (rc <= 0) break;
    sent += static_cast<std::size_t>(rc);
  }
  // Read the +OK reply before closing so the peer has time to process.
  char buf[32];
  (void)read(fd, buf, sizeof(buf));
  close(fd);
}

bool write_all(int fd, const char* data, std::size_t n) {
  std::size_t sent = 0;
  while (sent < n) {
    const auto rc = write(fd, data + sent, n - sent);
    if (rc <= 0) return false;
    sent += static_cast<std::size_t>(rc);
  }
  return true;
}

bool read_exact(int fd, char* out, std::size_t n) {
  std::size_t got = 0;
  while (got < n) {
    const auto rc = recv(fd, out + got, n - got, 0);
    if (rc <= 0) return false;
    got += static_cast<std::size_t>(rc);
  }
  return true;
}

bool read_line_crlf(int fd, std::string& line) {
  line.clear();
  char c = 0;
  while (true) {
    if (!read_exact(fd, &c, 1)) return false;
    if (c == '\r') {
      char lf = 0;
      if (!read_exact(fd, &lf, 1)) return false;
      if (lf != '\n') return false;
      return true;
    }
    line.push_back(c);
  }
}

bool read_resp_reply(int fd, std::string& payload, bool& is_err) {
  payload.clear();
  is_err = false;
  char prefix = 0;
  if (!read_exact(fd, &prefix, 1)) return false;
  std::string line;
  if (prefix == '+' || prefix == '-' || prefix == ':') {
    if (!read_line_crlf(fd, line)) return false;
    payload = line;
    is_err = prefix == '-';
    return true;
  }
  if (prefix == '$') {
    if (!read_line_crlf(fd, line)) return false;
    std::int64_t n = -1;
    if (!parse_i64(line, n)) return false;
    if (n == -1) return true;
    if (n < 0) return false;
    payload.resize(static_cast<std::size_t>(n));
    if (!read_exact(fd, payload.data(), static_cast<std::size_t>(n))) return false;
    char crlf[2];
    if (!read_exact(fd, crlf, 2)) return false;
    return crlf[0] == '\r' && crlf[1] == '\n';
  }
  return false;
}

std::string pseudo_sha1(const std::string& script) {
  auto leftrotate = [](std::uint32_t x, int n) -> std::uint32_t {
    return static_cast<std::uint32_t>((x << n) | (x >> (32 - n)));
  };
  std::vector<std::uint8_t> msg(script.begin(), script.end());
  const std::uint64_t bit_len = static_cast<std::uint64_t>(msg.size()) * 8ULL;
  msg.push_back(0x80);
  while ((msg.size() % 64) != 56) msg.push_back(0x00);
  for (int i = 7; i >= 0; --i) {
    msg.push_back(static_cast<std::uint8_t>((bit_len >> (i * 8)) & 0xff));
  }

  std::uint32_t h0 = 0x67452301U;
  std::uint32_t h1 = 0xEFCDAB89U;
  std::uint32_t h2 = 0x98BADCFEU;
  std::uint32_t h3 = 0x10325476U;
  std::uint32_t h4 = 0xC3D2E1F0U;

  for (std::size_t chunk = 0; chunk < msg.size(); chunk += 64) {
    std::uint32_t w[80]{};
    for (int i = 0; i < 16; ++i) {
      const std::size_t off = chunk + static_cast<std::size_t>(i) * 4;
      w[i] = (static_cast<std::uint32_t>(msg[off]) << 24) |
             (static_cast<std::uint32_t>(msg[off + 1]) << 16) |
             (static_cast<std::uint32_t>(msg[off + 2]) << 8) |
             static_cast<std::uint32_t>(msg[off + 3]);
    }
    for (int i = 16; i < 80; ++i) {
      w[i] = leftrotate(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }

    std::uint32_t a = h0, b = h1, c = h2, d = h3, e = h4;
    for (int i = 0; i < 80; ++i) {
      std::uint32_t f = 0;
      std::uint32_t k = 0;
      if (i < 20) {
        f = (b & c) | ((~b) & d);
        k = 0x5A827999U;
      } else if (i < 40) {
        f = b ^ c ^ d;
        k = 0x6ED9EBA1U;
      } else if (i < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8F1BBCDCU;
      } else {
        f = b ^ c ^ d;
        k = 0xCA62C1D6U;
      }
      const std::uint32_t temp = leftrotate(a, 5) + f + e + k + w[i];
      e = d;
      d = c;
      c = leftrotate(b, 30);
      b = a;
      a = temp;
    }
    h0 += a;
    h1 += b;
    h2 += c;
    h3 += d;
    h4 += e;
  }

  std::ostringstream oss;
  oss << std::hex << std::setfill('0') << std::nouppercase
      << std::setw(8) << h0
      << std::setw(8) << h1
      << std::setw(8) << h2
      << std::setw(8) << h3
      << std::setw(8) << h4;
  return oss.str();
}

std::uint16_t crc16_xmodem(std::string_view s) {
  std::uint16_t crc = 0;
  for (unsigned char c : s) {
    crc ^= static_cast<std::uint16_t>(c) << 8;
    for (int i = 0; i < 8; ++i) {
      if (crc & 0x8000) crc = static_cast<std::uint16_t>((crc << 1) ^ 0x1021);
      else crc = static_cast<std::uint16_t>(crc << 1);
    }
  }
  return crc;
}

int cluster_keyslot(const std::string& key) {
  const auto lb = key.find('{');
  if (lb != std::string::npos) {
    const auto rb = key.find('}', lb + 1);
    if (rb != std::string::npos && rb > lb + 1) {
      return crc16_xmodem(std::string_view(key).substr(lb + 1, rb - lb - 1)) % 16384;
    }
  }
  return crc16_xmodem(key) % 16384;
}

std::string eval_miniscript(const std::string& script, const std::vector<std::string>& keys, const std::vector<std::string>& argv) {
  // Simulate trivially-infinite scripts in a cooperative loop so that
  // other clients observe BUSY and can issue SCRIPT/FUNCTION KILL.
  if (script.find("while true do") != std::string::npos ||
      script.find("while 1 do") != std::string::npos) {
    g_script_kill_requested.store(false, std::memory_order_release);
    g_script_busy.store(true, std::memory_order_release);
    g_busy_script_session = g_script_current_session;
    return std::string("-BUSY Redis is busy running a script. Script killed by user with SCRIPT KILL.\r\n");
  }
  // Forward to real Lua 5.1 engine (replaces ~800 lines of hardcoded patterns).
  // Freeze time during script execution (Redis behavior).
  store().freeze_time();
  if (!g_script_current_session) {
    static SessionState dummy;
    lua_engine_set_readonly(g_script_readonly_context);
    auto result = lua_engine_eval(script, keys, argv, dummy);
    lua_engine_set_readonly(false);
    store().unfreeze_time();
    return result;
  }
  lua_engine_set_readonly(g_script_readonly_context);
  auto result = lua_engine_eval(script, keys, argv, *g_script_current_session);
  lua_engine_set_readonly(false);
  store().unfreeze_time();
  return result;
}

std::string hello_reply(const SessionState& session) {
  if (session.resp_version == RespVersion::Resp3) {
    return "%7\r\n+server\r\n$5\r\nredis\r\n+version\r\n$5\r\n7.2.5\r\n+proto\r\n:3\r\n+id\r\n:1\r\n+mode\r\n$10\r\nstandalone\r\n+role\r\n$6\r\nmaster\r\n+modules\r\n*0\r\n";
  }
  return "*14\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.5\r\n$5\r\nproto\r\n:2\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n";
}

bool parse_i64(const std::string& s, std::int64_t& out) {
  try {
    std::size_t idx = 0;
    out = std::stoll(s, &idx);
    return idx == s.size();
  } catch (...) {
    return false;
  }
}

bool parse_f64(const std::string& s, long double& out) {
  try {
    std::size_t idx = 0;
    out = std::stold(s, &idx);
    return idx == s.size();
  } catch (...) {
    return false;
  }
}

struct LcsMatchBlock {
  std::int64_t a_start = 0;
  std::int64_t a_end = 0;
  std::int64_t b_start = 0;
  std::int64_t b_end = 0;
  std::int64_t len = 0;
};

struct LcsResult {
  std::string lcs;
  std::vector<LcsMatchBlock> blocks_desc;
};

LcsResult compute_lcs(const std::string& a, const std::string& b) {
  const std::size_t n = a.size();
  const std::size_t m = b.size();
  std::vector<std::vector<std::uint32_t>> dp(n + 1, std::vector<std::uint32_t>(m + 1, 0));
  for (std::size_t i = 1; i <= n; ++i) {
    for (std::size_t j = 1; j <= m; ++j) {
      if (a[i - 1] == b[j - 1]) dp[i][j] = dp[i - 1][j - 1] + 1;
      else dp[i][j] = std::max(dp[i - 1][j], dp[i][j - 1]);
    }
  }

  std::size_t i = n;
  std::size_t j = m;
  std::vector<std::pair<std::int64_t, std::int64_t>> pairs_desc;
  while (i > 0 && j > 0) {
    if (a[i - 1] == b[j - 1] && dp[i][j] == dp[i - 1][j - 1] + 1) {
      pairs_desc.push_back({static_cast<std::int64_t>(i - 1), static_cast<std::int64_t>(j - 1)});
      --i;
      --j;
      continue;
    }
    if (dp[i - 1][j] >= dp[i][j - 1]) --i;
    else --j;
  }

  std::string out;
  out.reserve(pairs_desc.size());
  for (auto it = pairs_desc.rbegin(); it != pairs_desc.rend(); ++it) {
    out.push_back(a[static_cast<std::size_t>(it->first)]);
  }

  std::vector<LcsMatchBlock> blocks;
  if (!pairs_desc.empty()) {
    LcsMatchBlock cur{};
    cur.a_start = cur.a_end = pairs_desc[0].first;
    cur.b_start = cur.b_end = pairs_desc[0].second;
    for (std::size_t k = 1; k < pairs_desc.size(); ++k) {
      const auto [ai, bi] = pairs_desc[k];
      if (ai == cur.a_start - 1 && bi == cur.b_start - 1) {
        cur.a_start = ai;
        cur.b_start = bi;
      } else {
        cur.len = cur.a_end - cur.a_start + 1;
        blocks.push_back(cur);
        cur = {};
        cur.a_start = cur.a_end = ai;
        cur.b_start = cur.b_end = bi;
      }
    }
    cur.len = cur.a_end - cur.a_start + 1;
    blocks.push_back(cur);
  }
  return {out, blocks};
}

std::optional<std::int64_t> parse_expiry(std::string mode, const std::string& value, std::string& err,
                                         const std::string& cmd_name) {
  auto invalid_msg = [&]() { return "invalid expire time in '" + lower(cmd_name) + "' command"; };
  std::int64_t n = 0;
  if (!parse_i64(value, n)) {
    err = "value is not an integer or out of range";
    return std::nullopt;
  }
  auto mul_1000 = [&](std::int64_t x, std::int64_t& out) -> bool {
    if (x > std::numeric_limits<std::int64_t>::max() / 1000 ||
        x < std::numeric_limits<std::int64_t>::min() / 1000) {
      return false;
    }
    out = x * 1000;
    return true;
  };
  auto add_checked = [&](std::int64_t a, std::int64_t b, std::int64_t& out) -> bool {
    if ((b > 0 && a > std::numeric_limits<std::int64_t>::max() - b) ||
        (b < 0 && a < std::numeric_limits<std::int64_t>::min() - b)) {
      return false;
    }
    out = a + b;
    return true;
  };
  mode = upper(mode);
  if (mode == "EX") {
    if (n <= 0) {
      err = invalid_msg();
      return std::nullopt;
    }
    std::int64_t d = 0;
    if (!mul_1000(n, d)) {
      err = invalid_msg();
      return std::nullopt;
    }
    std::int64_t out = 0;
    if (!add_checked(now_ms(), d, out)) {
      err = invalid_msg();
      return std::nullopt;
    }
    return out;
  }
  if (mode == "PX") {
    if (n <= 0) {
      err = invalid_msg();
      return std::nullopt;
    }
    std::int64_t out = 0;
    if (!add_checked(now_ms(), n, out)) {
      err = invalid_msg();
      return std::nullopt;
    }
    return out;
  }
  if (mode == "EXAT") {
    std::int64_t out = 0;
    if (!mul_1000(n, out)) {
      err = invalid_msg();
      return std::nullopt;
    }
    return out;
  }
  if (mode == "PXAT") return n;
  err = "syntax error";
  return std::nullopt;
}

std::string dump_prefix() { return "PEADBSTR:"; }
std::string module_name_from_path(const std::string& p) {
  std::filesystem::path pp(p);
  return pp.stem().string();
}

extern "C" void* RedisModule_OpenKey(const char* key_name) {
  if (!key_name) return nullptr;
  g_module_keys.push_back(std::make_unique<ModuleKeyHandle>());
  g_module_keys.back()->key = key_name;
  return g_module_keys.back().get();
}

extern "C" int RedisModule_StringSet(void* key_ptr, const char* value) {
  if (!key_ptr || !value) return 1;
  auto* key = reinterpret_cast<ModuleKeyHandle*>(key_ptr);
  store().set(key->key, value, false, false, std::nullopt, false);
  return 0;
}

extern "C" const char* RedisModule_StringDMA(void* key_ptr, std::size_t* len, int) {
  if (!key_ptr) return nullptr;
  auto* key = reinterpret_cast<ModuleKeyHandle*>(key_ptr);
  auto v = store().get(key->key);
  if (!v.has_value()) return nullptr;
  g_module_dma_buf = *v;
  if (len) *len = g_module_dma_buf.size();
  return g_module_dma_buf.c_str();
}

extern "C" int RedisModule_CreateCommand(const char* name, void* cmd_func, const char*, int, int, int) {
  if (!name || !cmd_func || g_current_loading_module.empty()) return 1;
  const std::string cmd_name = upper(name);
  g_module_commands[cmd_name] = reinterpret_cast<ModuleCommandFn>(cmd_func);
  g_module_command_owner[cmd_name] = g_current_loading_module;
  return 0;
}

extern "C" int RedisModule_ReplyWithSimpleString(void* ctx, const char* msg) {
  if (!ctx || !msg) return 1;
  auto* c = reinterpret_cast<ModuleCallCtx*>(ctx);
  c->reply = encode_simple(msg);
  c->replied = true;
  return 0;
}

extern "C" int RedisModule_ReplyWithError(void* ctx, const char* msg) {
  if (!ctx || !msg) return 1;
  auto* c = reinterpret_cast<ModuleCallCtx*>(ctx);
  c->reply = encode_error(msg);
  c->replied = true;
  return 0;
}

extern "C" int RedisModule_ReplyWithLongLong(void* ctx, long long val) {
  if (!ctx) return 1;
  auto* c = reinterpret_cast<ModuleCallCtx*>(ctx);
  c->reply = encode_integer(val);
  c->replied = true;
  return 0;
}

extern "C" int RedisModule_ReplyWithDouble(void* ctx, double val) {
  if (!ctx) return 1;
  auto* c = reinterpret_cast<ModuleCallCtx*>(ctx);
  c->reply = encode_bulk(std::to_string(val));
  c->replied = true;
  return 0;
}

extern "C" int RedisModule_ReplyWithString(void* ctx, const char* str) {
  if (!ctx) return 1;
  auto* c = reinterpret_cast<ModuleCallCtx*>(ctx);
  c->reply = encode_bulk(str ? str : "");
  c->replied = true;
  return 0;
}

extern "C" int RedisModule_ReplyWithNull(void* ctx) {
  if (!ctx) return 1;
  auto* c = reinterpret_cast<ModuleCallCtx*>(ctx);
  c->reply = encode_null(RespVersion::Resp2);
  c->replied = true;
  return 0;
}

extern "C" int RedisModule_ReplyWithArray(void* ctx, long len) {
  if (!ctx) return 1;
  auto* c = reinterpret_cast<ModuleCallCtx*>(ctx);
  if (len == 0) {
    c->reply = "*0\r\n";
  } else {
    c->reply = "*" + std::to_string(len) + "\r\n";
  }
  c->replied = true;
  return 0;
}

extern "C" int RedisModule_ReplyWithStringBuffer(void* ctx, const char* buf, size_t len) {
  if (!ctx) return 1;
  auto* c = reinterpret_cast<ModuleCallCtx*>(ctx);
  std::string s(buf ? buf : "", buf ? len : 0);
  c->reply += encode_bulk(s);
  c->replied = true;
  return 0;
}

extern "C" int RedisModule_KeyType(void* key_ptr) {
  if (!key_ptr) return 0; // REDISMODULE_KEYTYPE_EMPTY
  auto* key = reinterpret_cast<ModuleKeyHandle*>(key_ptr);
  auto t = store().type_of(key->key);
  if (t == "none") return 0;    // EMPTY
  if (t == "string") return 1;  // STRING
  if (t == "list") return 2;    // LIST
  if (t == "hash") return 3;    // HASH
  if (t == "set") return 4;     // SET
  if (t == "zset") return 5;    // ZSET
  if (t == "stream") return 7;  // STREAM
  return 0;
}

extern "C" int RedisModule_DeleteKey(void* key_ptr) {
  if (!key_ptr) return 1;
  auto* key = reinterpret_cast<ModuleKeyHandle*>(key_ptr);
  store().del(key->key);
  return 0;
}

extern "C" void RedisModule_CloseKey(void*) {
  // No-op in our implementation — keys cleaned up at end of module call
}

extern "C" int RedisModule_StringToLongLong(const char* str, long long* val) {
  if (!str || !val) return 1;
  try {
    *val = std::stoll(str);
    return 0;
  } catch (...) {
    return 1;
  }
}

extern "C" int RedisModule_StringToDouble(const char* str, double* val) {
  if (!str || !val) return 1;
  try {
    *val = std::stod(str);
    return 0;
  } catch (...) {
    return 1;
  }
}

extern "C" const char* RedisModule_StringPtrLen(const char* str, size_t* len) {
  if (!str) {
    if (len) *len = 0;
    return "";
  }
  if (len) *len = strlen(str);
  return str;
}

extern "C" int RedisModule_HashSet(void* key_ptr, int flags, ...) {
  // Simplified: expects alternating field/value pairs, terminated by NULL
  // flags: REDISMODULE_HASH_NONE=0, REDISMODULE_HASH_NX=2, REDISMODULE_HASH_XX=4
  (void)flags;
  if (!key_ptr) return 1;
  // Can't properly handle varargs without knowing count; return error
  return 1;
}

extern "C" int RedisModule_HashGet(void* key_ptr, int flags, ...) {
  (void)flags;
  if (!key_ptr) return 1;
  return 1;
}

extern "C" void RedisModule_Log(void*, const char* level, const char* fmt, ...) {
  (void)level;
  (void)fmt;
  // Module logging — just ignore for now
}

extern "C" long long RedisModule_Milliseconds(void) {
  return static_cast<long long>(DataStore::now_ms());
}

extern "C" void* RedisModule_Alloc(size_t bytes) {
  return std::malloc(bytes);
}

extern "C" void* RedisModule_Realloc(void* ptr, size_t bytes) {
  return std::realloc(ptr, bytes);
}

extern "C" void RedisModule_Free(void* ptr) {
  std::free(ptr);
}

extern "C" char* RedisModule_Strdup(const char* str) {
  if (!str) return nullptr;
  size_t slen = strlen(str);
  char* dup = static_cast<char*>(std::malloc(slen + 1));
  if (dup) std::memcpy(dup, str, slen + 1);
  return dup;
}

extern "C" int RedisModule_Init(void*, const char*, int, int) {
  // Module initialization — always succeed
  return 0;
}

extern "C" void* RedisModule_GetApi(const char*) {
  // API lookup — not implemented, return null
  return nullptr;
}

template <typename Fn>
std::string with_string_key(const std::string& key, Fn&& fn) {
  if (store().is_wrongtype_for_string(key)) {
    return wrongtype_error_reply();
  }
  return fn();
}

template <typename Fn>
std::string with_hash_key(const std::string& key, Fn&& fn) {
  if (store().is_wrongtype_for_hash(key)) {
    return wrongtype_error_reply();
  }
  return fn();
}

template <typename Fn>
std::string with_list_key(const std::string& key, Fn&& fn) {
  if (store().is_wrongtype_for_list(key)) {
    return wrongtype_error_reply();
  }
  return fn();
}

template <typename Fn>
std::string with_set_key(const std::string& key, Fn&& fn) {
  if (store().is_wrongtype_for_set(key)) {
    return wrongtype_error_reply();
  }
  return fn();
}

template <typename Fn>
std::string with_zset_key(const std::string& key, Fn&& fn) {
  if (store().is_wrongtype_for_zset(key)) {
    return wrongtype_error_reply();
  }
  return fn();
}

template <typename Fn>
std::string with_stream_key(const std::string& key, Fn&& fn) {
  if (store().is_wrongtype_for_stream(key)) {
    return wrongtype_error_reply();
  }
  return fn();
}

const std::unordered_map<std::string, CommandSpec>& command_table() {
  static const std::unordered_map<std::string, CommandSpec> table = [] {
    std::unordered_map<std::string, CommandSpec> t;

    t.emplace("PING", CommandSpec{"PING", -1, {"fast", "readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (args.size() == 1) return encode_simple("PONG");
          if (args.size() == 2) return encode_bulk(args[1]);
          return encode_error("wrong number of arguments for 'ping' command");
        }});

    t.emplace("PUBLISH", CommandSpec{"PUBLISH", 3, {"write", "pubsub", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          auto receivers = pubsub_publish(args[1], args[2]);
          return encode_integer(static_cast<long long>(receivers));
        }});

    t.emplace("TIME", CommandSpec{"TIME", 1, {"readonly", "fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          const auto ms = DataStore::now_ms();
          const auto sec = ms / 1000;
          const auto usec = (ms % 1000) * 1000;
          return encode_array({encode_bulk(std::to_string(sec)), encode_bulk(std::to_string(usec))});
        }});

    t.emplace("ECHO", CommandSpec{"ECHO", 2, {"fast", "readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) { return encode_bulk(args[1]); }});

    t.emplace("QUIT", CommandSpec{"QUIT", 1, {"fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool& close) {
          close = true;
          return encode_simple("OK");
        }});

    t.emplace("SHUTDOWN", CommandSpec{"SHUTDOWN", -1, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool& close) {
          request_shutdown();
          close = true;
          return encode_simple("OK");
        }});

    t.emplace("SYNC", CommandSpec{"SYNC", 1, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) {
          session.replica_stream = true;
          session.repl_index = static_cast<std::size_t>(g_master_repl_offset);
          g_last_repl_db = -1; // Force re-emit SELECT on next write
          // Send empty RDB payload — replication stream carries all state.
          // RESP bulk string: $0\r\n followed by empty body (no trailing \r\n)
          return std::string("$0\r\n");
        }});

    t.emplace("REPLCONF", CommandSpec{"REPLCONF", -1, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          // REPLCONF listening-port <port>
          // REPLCONF capa eof|psync2
          // REPLCONF GETACK *
          // REPLCONF ACK <offset>
          for (std::size_t i = 1; i + 1 < args.size(); i += 2) {
            const auto opt = upper(args[i]);
            if (opt == "LISTENING-PORT") {
              // Store the replica's listening port (informational)
            } else if (opt == "CAPA") {
              // Note capability
            } else if (opt == "ACK") {
              // Parse replication offset from replica
              std::int64_t offset = 0;
              parse_i64(args[i + 1], offset);
              session.repl_ack_offset = offset;
              return std::string(); // No reply for ACK
            }
          }
          if (args.size() >= 2 && upper(args[1]) == "GETACK") {
            // Respond with our current offset
            return encode_array({
              encode_bulk("REPLCONF"),
              encode_bulk("ACK"),
              encode_bulk(std::to_string(g_master_repl_offset))
            });
          }
          return encode_simple("OK");
        }});

    t.emplace("PSYNC", CommandSpec{"PSYNC", 3, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          // PSYNC <replid> <offset>
          const std::string& replid = args[1];
          std::int64_t offset = -1;
          parse_i64(args[2], offset);

          session.replica_stream = true;
          g_last_repl_db = -1;

          // Attempt partial resync if replid matches and offset is within backlog
          if (replid == g_master_replid && offset >= 0 &&
              offset >= g_repl_backlog.base() && offset <= g_repl_backlog.total) {
            std::string pending;
            g_repl_backlog.get_slice(offset, pending);
            session.repl_index = static_cast<std::size_t>(g_master_repl_offset);
            return "+CONTINUE\r\n" + pending;
          }

          // Full resync
          std::string rdb_err;
          std::string rdb_data = rdb_save_to_string(rdb_err);
          if (!rdb_err.empty()) {
            // Cannot serialize dataset — refuse full sync rather than sending a corrupt RDB.
            log(LogLevel::Error, "PSYNC: full-resync aborted: " + rdb_err);
            session.replica_stream = false;
            return "-ERR Full sync failed: " + rdb_err + "\r\n";
          }
          session.repl_index = static_cast<std::size_t>(g_master_repl_offset);
          std::string reply = "+FULLRESYNC " + g_master_replid + " " +
                              std::to_string(g_master_repl_offset) + "\r\n";
          reply += "$" + std::to_string(rdb_data.size()) + "\r\n" + rdb_data;
          return reply;
        }});

    t.emplace("MULTI", CommandSpec{"MULTI", 1, {"fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) {
          if (session.in_multi) return encode_error("MULTI calls can not be nested");
          session.in_multi = true;
          session.multi_dirty = false;
          session.queued.clear();
          return encode_simple("OK");
        }});

    t.emplace("DISCARD", CommandSpec{"DISCARD", 1, {"fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) {
          if (!session.in_multi) return encode_error("DISCARD without MULTI");
          session.in_multi = false;
          session.multi_dirty = false;
          session.queued.clear();
          session.watched_epoch.reset();
          session.watched_digests.clear();
          return encode_simple("OK");
        }});

    t.emplace("WATCH", CommandSpec{"WATCH", -2, {"fast"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          if (session.in_multi) return encode_error("WATCH inside MULTI is not allowed");
          session.watched_epoch = store().mutation_epoch();
          for (std::size_t i = 1; i < args.size(); ++i) {
            session.watched_digests[args[i]] = store().debug_digest_value(args[i]);
          }
          return encode_simple("OK");
        }});

    t.emplace("UNWATCH", CommandSpec{"UNWATCH", 1, {"fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) {
          session.watched_epoch.reset();
          session.watched_digests.clear();
          return encode_simple("OK");
        }});

    t.emplace("EXEC", CommandSpec{"EXEC", 1, {"fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) {
          if (!session.in_multi) return encode_error("EXEC without MULTI");
          if (session.multi_dirty) {
            session.in_multi = false;
            session.multi_dirty = false;
            session.queued.clear();
            session.watched_epoch.reset();
            session.watched_digests.clear();
            return std::string("-EXECABORT Transaction discarded because of previous errors.\r\n");
          }
          if (!session.watched_digests.empty()) {
            bool changed = false;
            for (const auto& [key, digest] : session.watched_digests) {
              if (store().debug_digest_value(key) != digest) {
                changed = true;
                break;
              }
            }
            if (changed) {
              session.in_multi = false;
              session.multi_dirty = false;
              session.queued.clear();
              session.watched_epoch.reset();
              session.watched_digests.clear();
              return encode_null_array();
            }
          }

          const auto queued = session.queued;
          session.in_multi = false;
          session.multi_dirty = false;
          session.queued.clear();
          std::vector<std::string> replies;
          replies.reserve(queued.size());
          bool has_replof = false;
          for (const auto& q : queued) {
            if (!q.empty()) {
              const auto qc = upper(q[0]);
              if (qc == "REPLICAOF" || qc == "SLAVEOF") { has_replof = true; break; }
            }
          }
          g_capture_exec_replication = true;
          g_exec_replication_events.clear();
          g_exec_last_repl_db = g_last_repl_db;
          g_exec_write_event_count = 0;
          g_executing_exec = true;
          const auto& table = command_table();
          bool has_write = false;
          bool has_read = false;
          for (const auto& q : queued) {
            if (q.empty()) continue;
            const auto qc = upper(q[0]);
            auto it = table.find(qc);
            if (it == table.end()) continue;
            if (it->second.is_write()) has_write = true;
            else has_read = true;
          }
          if (has_write && g_config_min_replicas_to_write > 0) {
            g_executing_exec = false;
            g_capture_exec_replication = false;
            g_exec_replication_events.clear();
            g_exec_write_event_count = 0;
            session.watched_epoch.reset();
            session.watched_digests.clear();
            return std::string("-EXECABORT Transaction discarded because of previous errors: NOREPLICAS Not enough good replicas to write.\r\n");
          }
          if (has_write && g_config_maxmemory == 1) {
            g_executing_exec = false;
            g_capture_exec_replication = false;
            g_exec_replication_events.clear();
            g_exec_write_event_count = 0;
            session.watched_epoch.reset();
            session.watched_digests.clear();
            return std::string("-EXECABORT Transaction discarded because of previous errors: OOM command not allowed when used memory > 'maxmemory'.\r\n");
          }
          if (has_read && g_replication_role == "slave" && !g_config_replica_serve_stale_data && g_replica_stale) {
            g_executing_exec = false;
            g_capture_exec_replication = false;
            g_exec_replication_events.clear();
            g_exec_write_event_count = 0;
            session.watched_epoch.reset();
            session.watched_digests.clear();
            return std::string("-EXECABORT Transaction discarded because of previous errors: MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n");
          }
          for (const auto& q : queued) {
            bool close = false;
            replies.push_back(handle_command(q, session, close));
          }
          g_executing_exec = false;
          g_capture_exec_replication = false;
          if (!has_replof && g_exec_write_event_count > 1) {
            backlog_append(encode_command_resp({"MULTI"}));
            for (const auto& ev : g_exec_replication_events) backlog_append(ev);
            backlog_append(encode_command_resp({"EXEC"}));
            g_last_repl_db = g_exec_last_repl_db;
          } else if (!has_replof && g_exec_write_event_count == 1 && !g_exec_replication_events.empty()) {
            if (g_exec_last_repl_db != g_last_repl_db) {
              backlog_append(encode_command_resp({"SELECT", std::to_string(g_exec_last_repl_db)}));
            }
            backlog_append(g_exec_replication_events.back());
            g_last_repl_db = g_exec_last_repl_db;
          }
          g_exec_replication_events.clear();
          g_exec_write_event_count = 0;
          session.watched_epoch.reset();
          session.watched_digests.clear();
          return encode_array(replies);
        }});

    t.emplace("HELLO", CommandSpec{"HELLO", -1, {"fast", "no_auth"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          if (args.size() >= 2) {
            int proto = 0;
            try {
              proto = std::stoi(args[1]);
            } catch (...) {
              return encode_error("Protocol version is not an integer or out of range");
            }
            if (proto != 2 && proto != 3) return encode_error("NOPROTO unsupported protocol version");
            session.resp_version = (proto == 3 ? RespVersion::Resp3 : RespVersion::Resp2);
          }
          return hello_reply(session);
        }});

    t.emplace("AUTH", CommandSpec{"AUTH", -2, {"fast", "no_auth"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (args.size() == 2 || args.size() == 3) return encode_simple("OK");
          return encode_error("wrong number of arguments for 'auth' command");
        }});

    t.emplace("ACL", CommandSpec{"ACL", -2, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (upper(args[1]) == "SETUSER") return encode_simple("OK");
          return encode_error("unknown subcommand for ACL");
        }});

    t.emplace("SCRIPT", CommandSpec{"SCRIPT", -2, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          const std::string sub = upper(args[1]);
          if (sub == "LOAD" && args.size() == 3) {
            const auto sha = pseudo_sha1(args[2]);
            g_script_cache[sha] = args[2];
            return encode_bulk(sha);
          }
          if (sub == "EXISTS" && args.size() >= 3) {
            std::vector<std::string> out;
            for (std::size_t i = 2; i < args.size(); ++i) {
              out.push_back(encode_integer(g_script_cache.count(lower(args[i])) ? 1 : 0));
            }
            return encode_array(out);
          }
          if (sub == "FLUSH" && args.size() == 2) {
            g_script_cache.clear();
            return encode_simple("OK");
          }
          if (sub == "FLUSH" && args.size() == 3 &&
              (upper(args[2]) == "SYNC" || upper(args[2]) == "ASYNC")) {
            g_script_cache.clear();
            return encode_simple("OK");
          }
          if (sub == "KILL" && args.size() == 2) {
            if (!g_script_busy.load()) {
              return std::string("-NOTBUSY No scripts in execution right now.\r\n");
            }
            g_script_kill_requested.store(true);
            g_script_busy.store(false);
            g_busy_script_session = nullptr;
            return encode_simple("OK");
          }
          return encode_error("unknown subcommand for SCRIPT");
        }});

    t.emplace("EVAL", CommandSpec{"EVAL", -3, {"write", "noscript"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          struct ShebangInfo {
            bool has_shebang = false;
            bool explicit_flags = false;
            bool allow_oom = false;
            bool no_writes = false;
            std::string body;
            std::string err;
          };
          auto parse_shebang = [](const std::string& src) -> ShebangInfo {
            ShebangInfo out;
            out.body = src;
            if (src.rfind("#!", 0) != 0) return out;
            out.has_shebang = true;
            const auto nl = src.find('\n');
            const std::string hdr = src.substr(2, nl == std::string::npos ? std::string::npos : nl - 2);
            out.body = nl == std::string::npos ? std::string() : src.substr(nl + 1);
            std::istringstream iss(hdr);
            std::string engine;
            iss >> engine;
            if (lower(engine) != "lua") {
              out.err = "Unexpected engine in script shebang";
              return out;
            }
            std::string tok;
            while (iss >> tok) {
              if (tok.rfind("flags=", 0) != 0) {
                out.err = "Unknown lua shebang option";
                return out;
              }
              out.explicit_flags = true;
              const auto flags = tok.substr(6);
              if (flags.empty()) continue;
              std::size_t start = 0;
              while (start <= flags.size()) {
                const auto comma = flags.find(',', start);
                const auto piece = flags.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
                if (piece == "allow-oom") out.allow_oom = true;
                else if (piece == "no-writes") out.no_writes = true;
                else {
                  out.err = "Unexpected flag in script shebang";
                  return out;
                }
                if (comma == std::string::npos) break;
                start = comma + 1;
              }
            }
            return out;
          };
          auto script_has_write_intent = [](const std::string& src) -> bool {
            const auto nsl = lower(normalize_script(src));
            const std::vector<std::string> markers = {
              "redis.call('set'", "redis.call(\"set\"",
              "redis.call('del'", "redis.call(\"del\"",
              "redis.call('incr'", "redis.call(\"incr\"",
              "redis.call('decr'", "redis.call(\"decr\"",
              "redis.call('expire'", "redis.call(\"expire\"",
              "redis.call('pexpire'", "redis.call(\"pexpire\"",
              "redis.call('sadd'", "redis.call(\"sadd\"",
              "redis.call('srem'", "redis.call(\"srem\"",
              "redis.call('spop'", "redis.call(\"spop\"",
              "redis.call('incrbyfloat'", "redis.call(\"incrbyfloat\""
            };
            for (const auto& m : markers) {
              if (nsl.find(m) != std::string::npos) return true;
            }
            return false;
          };
          std::int64_t numkeys = 0;
          if (!parse_i64(args[2], numkeys)) {
            return encode_error("value is not an integer or out of range");
          }
          if (numkeys < 0) return encode_error("Number of keys can't be negative");
          if (args.size() < static_cast<std::size_t>(3 + numkeys)) {
            return encode_error("Number of keys can't be greater than number of args");
          }
          std::vector<std::string> keys;
          std::vector<std::string> argv;
          for (std::int64_t i = 0; i < numkeys; ++i) keys.push_back(args[3 + i]);
          for (std::size_t i = static_cast<std::size_t>(3 + numkeys); i < args.size(); ++i) argv.push_back(args[i]);

          const auto she = parse_shebang(args[1]);
          if (!she.err.empty()) return encode_error(she.err);
          const std::string script_body = she.has_shebang ? she.body : args[1];
          const bool allow_oom = she.allow_oom || she.no_writes;
          const bool write_intent = she.has_shebang ? !she.no_writes : script_has_write_intent(script_body);
          if (g_config_maxmemory == 1) {
            if (she.has_shebang && !she.explicit_flags && !allow_oom) {
              return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
            }
            if (she.explicit_flags && !allow_oom) {
              return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
            }
          }
          if (write_intent && g_config_min_replicas_to_write > 0) {
            return std::string("-NOREPLICAS Not enough good replicas to write.\r\n");
          }

          const auto sha = pseudo_sha1(args[1]);
          g_script_cache[sha] = args[1];
          g_script_client_resp_version = session.resp_version;
          g_script_current_session = &session;
          const bool prev_readonly = g_script_readonly_context;
          const bool prev_allow_oom = g_script_allow_oom;
          if (she.no_writes) g_script_readonly_context = true;
          if (allow_oom) g_script_allow_oom = true;
          const auto out = eval_miniscript(script_body, keys, argv);
          g_script_readonly_context = prev_readonly;
          g_script_allow_oom = prev_allow_oom;
          g_script_current_session = nullptr;
          return out;
        }});

    t.emplace("EVALSHA", CommandSpec{"EVALSHA", -3, {"write", "noscript"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          auto it = g_script_cache.find(lower(args[1]));
          if (it == g_script_cache.end()) {
            return std::string("-NOSCRIPT No matching script. Please use EVAL.\r\n");
          }
          std::int64_t numkeys = 0;
          if (!parse_i64(args[2], numkeys)) {
            return encode_error("value is not an integer or out of range");
          }
          if (numkeys < 0) return encode_error("Number of keys can't be negative");
          if (args.size() < static_cast<std::size_t>(3 + numkeys)) {
            return encode_error("Number of keys can't be greater than number of args");
          }
          std::vector<std::string> keys;
          std::vector<std::string> argv;
          for (std::int64_t i = 0; i < numkeys; ++i) keys.push_back(args[3 + i]);
          for (std::size_t i = static_cast<std::size_t>(3 + numkeys); i < args.size(); ++i) argv.push_back(args[i]);
          g_script_client_resp_version = session.resp_version;
          g_script_current_session = &session;
          const auto out = eval_miniscript(it->second, keys, argv);
          g_script_current_session = nullptr;
          return out;
        }});

    t.emplace("EVAL_RO", CommandSpec{"EVAL_RO", -3, {"readonly", "noscript"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool& close) {
          std::vector<std::string> proxy = args;
          proxy[0] = "EVAL";
          g_script_readonly_context = true;
          const auto& tbl = command_table();
          const auto it = tbl.find("EVAL");
          const auto out = (it == tbl.end()) ? encode_error("unknown command") : it->second.handler(proxy, session, close);
          g_script_readonly_context = false;
          return out;
        }});

    t.emplace("EVALSHA_RO", CommandSpec{"EVALSHA_RO", -3, {"readonly", "noscript"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool& close) {
          std::vector<std::string> proxy = args;
          proxy[0] = "EVALSHA";
          const auto& tbl = command_table();
          const auto it = tbl.find("EVALSHA");
          return (it == tbl.end()) ? encode_error("unknown command") : it->second.handler(proxy, session, close);
        }});

    t.emplace("INFO", CommandSpec{"INFO", -1, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          const auto section = (args.size() >= 2) ? upper(args[1]) : std::string("ALL");
          auto uptime_s = (now_ms() - g_server_start_time.load()) / 1000;
          if (uptime_s < 1) uptime_s = 1;

          const std::size_t used_mem = static_cast<std::size_t>(store().estimate_used_memory());

          auto format_mem = [](std::size_t bytes) -> std::string {
            if (bytes >= 1024ULL * 1024 * 1024) return std::to_string(bytes / (1024ULL * 1024 * 1024)) + "G";
            if (bytes >= 1024ULL * 1024) return std::to_string(bytes / (1024ULL * 1024)) + "M";
            if (bytes >= 1024ULL) return std::to_string(bytes / 1024) + "K";
            return std::to_string(bytes) + "B";
          };

          std::string out;
          auto want = [&](const std::string& s) {
            return section == "ALL" || section == "EVERYTHING" || section == s;
          };

          if (want("SERVER")) {
            out += "# Server\r\n"
                   "redis_version:7.2.5\r\n"
                   "peadb_git_sha1:00000000\r\n"
                   "peadb_git_dirty:0\r\n"
                   "redis_build_id:peadb-dev\r\n"
                   "peadb_version:0.1.0\r\n"
                   "redis_mode:standalone\r\n"
                   "os:Linux\r\n"
                   "arch_bits:64\r\n"
                   "tcp_port:" + std::to_string(g_server_port) + "\r\n"
                   "uptime_in_seconds:" + std::to_string(uptime_s) + "\r\n"
                   "uptime_in_days:" + std::to_string(uptime_s / 86400) + "\r\n"
                   "executable:peadb-server\r\n"
                   "config_file:\r\n"
                   "cluster_enabled:" + std::string(g_cluster_peers.empty() ? "0" : "1") + "\r\n\r\n";
          }
          if (want("CLIENTS")) {
            out += "# Clients\r\n"
                   "connected_clients:" + std::to_string(g_connected_clients.load()) + "\r\n"
                   "blocked_clients:0\r\n"
                   "tracking_clients:0\r\n"
                   "maxclients:" + std::to_string(g_runtime_max_clients.load(std::memory_order_relaxed)) + "\r\n\r\n";
          }
          if (want("MEMORY")) {
            out += "# Memory\r\n"
                   "used_memory:" + std::to_string(used_mem) + "\r\n"
                   "used_memory_human:" + format_mem(used_mem) + "\r\n"
                   "used_memory_peak:" + std::to_string(used_mem) + "\r\n"
                   "used_memory_peak_human:" + format_mem(used_mem) + "\r\n"
                   "used_memory_rss:" + std::to_string(used_mem) + "\r\n"
                   "mem_fragmentation_ratio:1.00\r\n"
                   "maxmemory:" + std::to_string(g_config_maxmemory) + "\r\n"
                   "maxmemory_human:" + format_mem(static_cast<size_t>(g_config_maxmemory)) + "\r\n"
                   "maxmemory_policy:noeviction\r\n"
                   "number_of_cached_scripts:" + std::to_string(g_script_cache.size()) + "\r\n\r\n";
          }
          if (want("PERSISTENCE")) {
            out += "# Persistence\r\n"
                   "loading:0\r\n"
                   "rdb_changes_since_last_save:" + std::to_string(store().mutation_epoch()) + "\r\n"
                   "rdb_bgsave_in_progress:" + std::to_string(g_bgsave_in_progress.load() ? 1 : 0) + "\r\n"
                   "rdb_last_save_time:" + std::to_string(g_last_save_time) + "\r\n"
                   "rdb_last_bgsave_status:" + g_last_bgsave_status + "\r\n"
                   "aof_enabled:" + std::to_string(g_appendonly ? 1 : 0) + "\r\n"
                   "aof_rewrite_in_progress:" + std::to_string(g_bgrewrite_in_progress.load() ? 1 : 0) + "\r\n"
                   "aof_last_bgrewrite_status:ok\r\n"
                   "aof_write_error:" + std::to_string(g_aof_write_error ? 1 : 0) + "\r\n\r\n";
          }
          if (want("STATS")) {
            out += "# Stats\r\n"
                   "total_connections_received:" + std::to_string(g_total_connections_received.load()) + "\r\n"
                   "total_commands_processed:" + std::to_string(g_total_commands_processed.load()) + "\r\n"
                   "total_error_replies:" + std::to_string(g_total_error_replies) + "\r\n"
                   "keyspace_hits:" + std::to_string(store().stat_keyspace_hits()) + "\r\n"
                   "keyspace_misses:" + std::to_string(store().stat_keyspace_misses()) + "\r\n"
                   "expired_keys:" + std::to_string(store().stat_expired_keys()) + "\r\n"
                   "evicted_keys:" + std::to_string(store().stat_evicted_keys()) + "\r\n\r\n";
          }
          if (want("REPLICATION")) {
            out += "# Replication\r\n"
                   "role:" + g_replication_role + "\r\n"
                   "connected_slaves:" + std::to_string(g_connected_replicas.load()) + "\r\n"
                   "master_failover_state:no-failover\r\n"
                   "master_replid:" + g_master_replid + "\r\n"
                   "master_replid2:0000000000000000000000000000000000000000\r\n"
                   "master_repl_offset:" + std::to_string(g_master_repl_offset) + "\r\n"
                   "repl_backlog_active:0\r\n"
                   "repl_backlog_size:1048576\r\n";
            if (g_replication_role != "master") {
              out += "master_host:" + g_replication_master_host + "\r\n"
                     "master_port:" + std::to_string(g_replication_master_port) + "\r\n"
                     "master_link_status:up\r\n";
            }
            out += "\r\n";
          }
          if (want("CPU")) {
            out += "# CPU\r\n"
                   "used_cpu_sys:0.000000\r\n"
                   "used_cpu_user:0.000000\r\n\r\n";
          }
          if (want("MODULES")) {
            out += "# Modules\r\n\r\n";
          }
          if (want("ERRORSTATS")) {
            out += "# Errorstats\r\n";
            for (const auto& [k, v] : g_errorstats) {
              out += "errorstat_" + upper(k) + ":count=" + std::to_string(v) + "\r\n";
            }
            out += "\r\n";
          }
          if (want("COMMANDSTATS")) {
            out += "# Commandstats\r\n";
            for (const auto& [k, st] : g_cmdstats) {
              out += "cmdstat_" + lower(k) +
                     ":calls=" + std::to_string(st.calls) +
                     ",usec=1,usec_per_call=1.00,rejected_calls=" + std::to_string(st.rejected_calls) +
                     ",failed_calls=" + std::to_string(st.failed_calls) + "\r\n";
            }
            out += "\r\n";
          }
          if (want("KEYSPACE")) {
            out += "# Keyspace\r\n";
            const int prev = store().current_db();
            for (int d = 0; d < 16; ++d) {
              store().select_db(d);
              const auto [keys_count, expires_count, avg_ttl] = store().keyspace_stats();
              if (keys_count > 0) {
                out += "db" + std::to_string(d) + ":keys=" + std::to_string(keys_count) +
                       ",expires=" + std::to_string(expires_count) +
                       ",avg_ttl=" + std::to_string(avg_ttl) + "\r\n";
              }
            }
            store().select_db(prev);
            out += "\r\n";
          }

          if (out.empty()) {
            // Unknown section — return server info as default
            out = "# Server\r\nredis_version:7.2.5\r\n";
          }
          return encode_bulk(out);
        }});

    t.emplace("SAVE", CommandSpec{"SAVE", 1, {"admin", "noscript"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          std::string err;
          if (!rdb_save(g_snapshot_path, err)) return encode_error(err);
          g_last_save_time = now_ms() / 1000;
          return encode_simple("OK");
        }});

    t.emplace("BGSAVE", CommandSpec{"BGSAVE", 1, {"admin", "noscript"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          if (g_bgsave_in_progress.load()) return encode_error("Background save already in progress");
          pid_t pid = fork();
          if (pid < 0) return encode_error("Background save failed: fork error");
          if (pid == 0) {
            std::string err;
            bool ok = rdb_save(g_snapshot_path, err);
            _exit(ok ? 0 : 1);
          }
          g_bgsave_in_progress.store(true);
          g_bgsave_child_pid.store(pid);
          g_bgsave_start_time = now_ms();
          return encode_simple("Background saving started");
        }});

    t.emplace("BGREWRITEAOF", CommandSpec{"BGREWRITEAOF", 1, {"admin", "noscript"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          if (g_bgrewrite_in_progress.load())
            return encode_error("Background append only file rewriting already in progress");
          pid_t pid = fork();
          if (pid < 0) return encode_error("BGREWRITEAOF failed: fork error");
          if (pid == 0) {
            std::string err;
            bool ok = rewrite_aof_file(err);
            _exit(ok ? 0 : 1);
          }
          g_bgrewrite_in_progress.store(true);
          g_bgrewrite_child_pid.store(pid);
          return encode_simple("Background append only file rewriting started");
        }});

    t.emplace("WAIT", CommandSpec{"WAIT", 3, {"noscript"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::int64_t replicas = 0;
          std::int64_t timeout = 0;
          if (!parse_i64(args[1], replicas) || !parse_i64(args[2], timeout) || replicas < 0 || timeout < 0) {
            return encode_error("value is not an integer or out of range");
          }
          // In Lua script context: return current count immediately (no blocking)
          if (g_in_lua_script) {
            if (g_count_synced_replicas) {
              return encode_integer(g_count_synced_replicas(g_master_repl_offset));
            }
            return encode_integer(g_connected_replicas.load());
          }
          // Check immediately — snapshot offset at call time (Redis semantics)
          const std::int64_t target = g_master_repl_offset;
          if (g_count_synced_replicas) {
            int synced = g_count_synced_replicas(target);
            if (synced >= replicas) return encode_integer(synced);
          } else {
            return encode_integer(0);
          }
          if (replicas == 0) return encode_integer(0);
          // Defer: event loop resolves when replicas ack or deadline passes
          const auto deadline = (timeout > 0) ? (now_ms() + timeout) : std::int64_t{0};
          session.wait_state = WaitState{target, deadline, replicas};
          return std::string{};
        }});

    t.emplace("REPLICAOF", CommandSpec{"REPLICAOF", 3, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          const std::string host = args[1];
          const std::string port = args[2];
          if (upper(host) == "NO" && upper(port) == "ONE") {
            g_replication_role = "master";
            g_replication_master_host.clear();
            g_replication_master_port = 0;
            g_replica_stale = false;
            g_loading_replication.store(false);
            return encode_simple("OK");
          }
          std::int64_t p = 0;
          if (!parse_i64(port, p) || p < 0 || p > 65535) return encode_error("value is not an integer or out of range");
          g_replication_master_host = host;
          g_replication_master_port = static_cast<int>(p);
          g_replication_role = "slave";
          g_replica_stale = true;
          if (p == 0) {
            return encode_simple("OK");
          }
          // Threading invariant: this thread accesses only atomics (g_loading_replication,
          // g_replica_stale) and the DataStore under the implicit "event loop blocks client
          // writes while g_loading_replication is true" guarantee (enforced at the top of
          // handle_command). It must NOT call into Lua engine, script-context globals, or
          // session state owned by the event loop.
          std::thread([host, p]() {
            // Small delay to ensure pending client commands see slave role first
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            g_loading_replication.store(true);
            const bool ok = replica_do_fullsync(host, static_cast<int>(p));
            g_loading_replication.store(false);
            if (ok) g_replica_stale = false;
          }).detach();
          return encode_simple("OK");
        }});

    t.emplace("SLAVEOF", CommandSpec{"SLAVEOF", 3, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool& close) {
          std::vector<std::string> proxy = {"REPLICAOF", args[1], args[2]};
          return handle_command(proxy, session, close);
        }});

    t.emplace("CLUSTER", CommandSpec{"CLUSTER", -2, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          const auto sub = upper(args[1]);
          // Note: RedisInsight / ioredis detect cluster mode via
          // cluster_enabled:0 in INFO server — not via CLUSTER SLOTS.
          // All cluster subcommands remain available so users can set up
          // clustering (CLUSTER MEET / SETSLOT) and tests can inspect state.
          if (sub == "KEYSLOT" && args.size() == 3) {
            return encode_integer(cluster_keyslot(args[2]));
          }
          if (sub == "MEET" && args.size() == 4) {
            std::int64_t port = 0;
            if (!parse_i64(args[3], port) || port <= 0 || port > 65535) {
              return encode_error("Invalid cluster port");
            }
            const std::string peer = args[2] + ":" + std::to_string(port);
            g_cluster_peers.insert(peer);
            try_send_cluster_meetback(args[2], static_cast<int>(port));
            return encode_simple("OK");
          }
          if (sub == "MEETBACK" && args.size() == 4) {
            std::int64_t port = 0;
            if (!parse_i64(args[3], port) || port <= 0 || port > 65535) {
              return encode_error("Invalid cluster port");
            }
            const std::string peer = args[2] + ":" + std::to_string(port);
            g_cluster_peers.insert(peer);
            return encode_simple("OK");
          }
          if (sub == "SETSLOT" && args.size() >= 5) {
            std::int64_t slot = -1;
            if (!parse_i64(args[2], slot) || slot < 0 || slot >= 16384) return encode_error("Invalid slot");
            const std::string mode = upper(args[3]);
            if (mode == "NODE") {
              const std::string node = lower(args[4]);
              g_slot_routes[static_cast<std::size_t>(slot)] = (node == "self") ? SlotRoute::Owned : SlotRoute::Moved;
              return encode_simple("OK");
            }
            if (mode == "MIGRATING" || mode == "IMPORTING") {
              g_slot_routes[static_cast<std::size_t>(slot)] = SlotRoute::Ask;
              return encode_simple("OK");
            }
            return encode_error("Unsupported CLUSTER SETSLOT mode");
          }
          if (sub == "INFO") {
            // In standalone mode (no peers), return an error matching Redis.
            if (g_cluster_peers.empty()) {
              return std::string("-ERR This instance has cluster support disabled\r\n");
            }
            // In cluster mode, report full cluster info.
            bool cluster_active = !g_cluster_peers.empty();
            std::size_t cluster_size = cluster_active ? (1 + g_cluster_peers.size()) : 0;
            std::size_t slots_assigned = cluster_active ? 16384 : 0;
            std::size_t slots_ok = cluster_active ? 16384 : 0;
            return encode_bulk(
                "cluster_enabled:" + std::string(cluster_active ? "1" : "0") + "\r\n"
                "cluster_state:ok\r\n"
                "cluster_slots_assigned:" + std::to_string(slots_assigned) + "\r\n"
                "cluster_slots_ok:" + std::to_string(slots_ok) + "\r\n"
                "cluster_slots_pfail:0\r\n"
                "cluster_slots_fail:0\r\n"
                "cluster_known_nodes:" + std::to_string(cluster_active ? cluster_size : 1) + "\r\n"
                "cluster_size:" + std::to_string(cluster_size) + "\r\n"
                "cluster_current_epoch:1\r\n"
                "cluster_my_epoch:1\r\n"
                "cluster_stats_messages_sent:0\r\n"
                "cluster_stats_messages_received:0\r\n"
                "total_cluster_links_buffer_limit_exceeded:0\r\n");
          }
          if (sub == "NODES") {
            std::string out = g_master_replid + " 127.0.0.1:" + std::to_string(g_server_port) +
                              "@" + std::to_string(g_server_port + 10000) +
                              " myself,master - 0 0 1 connected 0-16383\r\n";
            std::size_t idx = 2;
            for (const auto& peer : g_cluster_peers) {
              std::ostringstream id;
              id << std::hex << std::setw(40) << std::setfill('0') << idx++;
              // Parse host:port from peer
              auto colon = peer.rfind(':');
              std::string phost = (colon != std::string::npos) ? peer.substr(0, colon) : peer;
              std::string pport = (colon != std::string::npos) ? peer.substr(colon + 1) : "6379";
              int pp = 6379;
              try { pp = std::stoi(pport); } catch (...) {}
              out += id.str() + " " + phost + ":" + pport +
                     "@" + std::to_string(pp + 10000) + " master - 0 0 1 connected\r\n";
            }
            return encode_bulk(out);
          }
          if (sub == "MYID") {
            return encode_bulk(g_master_replid);
          }
          if (sub == "SLOTS") {
            // In standalone mode (no peers), return an error matching Redis
            // behaviour.  This prevents ioredis/RedisInsight from switching
            // to cluster connection mode.
            if (g_cluster_peers.empty()) {
              return std::string("-ERR This instance has cluster support disabled\r\n");
            }
            return std::string("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:" + std::to_string(g_server_port) + "\r\n");
          }
          if (sub == "SHARDS") {
            if (g_cluster_peers.empty()) {
              return std::string("-ERR This instance has cluster support disabled\r\n");
            }
            return encode_array({});
          }
          return encode_error("unknown subcommand for CLUSTER");
        }});

    t.emplace("ASKING", CommandSpec{"ASKING", 1, {"fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) {
          session.asking = true;
          return encode_simple("OK");
        }});

    t.emplace("COMMAND", CommandSpec{"COMMAND", -1, {"loading", "stale"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          const auto& table = command_table();
          if (args.size() == 1) return encode_array({});
          const std::string sub = upper(args[1]);
          if (sub == "COUNT") return encode_integer(static_cast<long long>(table.size()));
          if (sub == "INFO") {
            if (args.size() == 2) return encode_array({});
            std::vector<std::string> out;
            for (std::size_t i = 2; i < args.size(); ++i) {
              const std::string k = upper(args[i]);
              auto it = table.find(k);
              if (it == table.end()) {
                out.push_back(encode_null(session.resp_version));
                continue;
              }
              const auto& s = it->second;
              std::vector<std::string> flags;
              for (const auto& f : s.flags) flags.push_back(encode_bulk(f));
              out.push_back(encode_array({
                  encode_bulk(s.name), encode_integer(s.arity), encode_array(flags),
                  encode_integer(s.first_key), encode_integer(s.last_key), encode_integer(s.key_step),
              }));
            }
            return encode_array(out);
          }
          if (sub == "DOCS") return session.resp_version == RespVersion::Resp3 ? std::string("%0\r\n") : std::string("*0\r\n");
          return encode_error("unknown subcommand for COMMAND");
        }});

    t.emplace("CONFIG", CommandSpec{"CONFIG", -2, {"admin", "loading"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (args.size() >= 3 && upper(args[1]) == "GET") {
            const auto& pattern = args[2];
            // Build config parameter table
            std::vector<std::pair<std::string, std::string>> config_params = {
              {"save", ""},
              {"appendonly", g_appendonly ? "yes" : "no"},
              {"maxmemory", std::to_string(g_config_maxmemory)},
              {"min-replicas-to-write", std::to_string(g_config_min_replicas_to_write)},
              {"replica-serve-stale-data", g_config_replica_serve_stale_data ? "yes" : "no"},
              {"lua-time-limit", std::to_string(g_config_lua_time_limit)},
              {"zset-max-ziplist-entries", std::to_string(store().zset_max_ziplist_entries())},
              {"zset-max-listpack-entries", std::to_string(store().zset_max_ziplist_entries())},
              {"dbfilename", g_snapshot_path.filename().string()},
              {"dir", std::filesystem::current_path().string()},
              {"bind", "0.0.0.0"},
              {"port", std::to_string(g_server_port)},
              {"hz", "10"},
              {"lfu-log-factor", "10"},
              {"lfu-decay-time", "1"},
              {"slave-serve-stale-data", g_config_replica_serve_stale_data ? "yes" : "no"},
              {"min-slaves-to-write", std::to_string(g_config_min_replicas_to_write)},
              {"slowlog-log-slower-than", "10000"},
              {"slowlog-max-len", "128"},
              {"databases", "16"},
              {"activedefrag", "no"},
              {"loglevel", "notice"},
              {"maxmemory-policy", "noeviction"},
              {"timeout", "0"},
              {"tcp-keepalive", "300"},
              {"list-max-ziplist-size", "-2"},
              {"set-max-intset-entries", "512"},
              {"maxclients", std::to_string(g_runtime_max_clients.load(std::memory_order_relaxed))},
              {"appendfsync", g_appendfsync},
              {"appendfilename", g_aof_path.filename().string()},
            };
            std::vector<std::string> out;
            for (const auto& [key, value] : config_params) {
              if (fnmatch(pattern.c_str(), key.c_str(), FNM_CASEFOLD) == 0) {
                out.push_back(encode_bulk(key));
                out.push_back(encode_bulk(value));
              }
            }
            return encode_array(out);
          }
          if (args.size() >= 4 && upper(args[1]) == "SET") {
            const auto k = lower(args[2]);
            if (k == "maxmemory") {
              std::int64_t v = 0;
              if (!parse_i64(args[3], v) || v < 0) {
                return encode_error("CONFIG SET failed (possibly related to argument '" + args[2] + "') - argument couldn't be parsed into an integer");
              }
              g_config_maxmemory = v;
            }
            if (k == "maxclients") {
              std::int64_t v = 0;
              if (!parse_i64(args[3], v) || v < 1 || v > std::numeric_limits<int>::max()) {
                return encode_error("value is not an integer or out of range");
              }
              g_runtime_max_clients.store(static_cast<int>(v), std::memory_order_relaxed);
            }
            if (k == "min-replicas-to-write") {
              std::int64_t v = 0;
              if (!parse_i64(args[3], v) || v < 0) return encode_error("value is not an integer or out of range");
              g_config_min_replicas_to_write = v;
            }
            if (k == "replica-serve-stale-data") {
              const auto v = lower(args[3]);
              if (v != "yes" && v != "no") return encode_error("argument must be 'yes' or 'no'");
              g_config_replica_serve_stale_data = (v == "yes");
            }
            if (k == "lua-time-limit") {
              std::int64_t v = 0;
              if (!parse_i64(args[3], v) || v < 0) return encode_error("value is not an integer or out of range");
              g_config_lua_time_limit = v;
            }
            if (k == "zset-max-ziplist-entries" || k == "zset-max-listpack-entries") {
              std::int64_t v = 0;
              if (!parse_i64(args[3], v) || v < 0) return encode_error("value is not an integer or out of range");
              store().set_zset_max_ziplist_entries(v);
            }
            if (k == "appendfsync") {
              const auto v = lower(args[3]);
              if (v != "always" && v != "everysec" && v != "no")
                return encode_error("Invalid argument '" + args[3] + "' for CONFIG SET 'appendfsync'");
              g_appendfsync = v;
            }
            return encode_simple("OK");
          }
          if (args.size() == 2 && upper(args[1]) == "RESETSTAT") {
            g_cmdstats.clear();
            g_errorstats.clear();
            g_total_error_replies = 0;
            return encode_simple("OK");
          }
          return encode_error("wrong number of arguments for 'config' command");
        }});

    t.emplace("DEBUG", CommandSpec{"DEBUG", -2, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (args.size() == 3 && upper(args[1]) == "SET-ACTIVE-EXPIRE") {
            std::int64_t v = 0;
            if (!parse_i64(args[2], v) || (v != 0 && v != 1)) return encode_error("value is not an integer or out of range");
            g_active_expire = (v == 1);
            return encode_simple("OK");
          }
          if (args.size() == 3 && upper(args[1]) == "SET-DISABLE-DENY-SCRIPTS") {
            std::int64_t v = 0;
            if (!parse_i64(args[2], v) || (v != 0 && v != 1)) return encode_error("value is not an integer or out of range");
            return encode_simple("OK");
          }
          if (args.size() == 3 && upper(args[1]) == "DIGEST-VALUE") {
            auto v = store().debug_digest_value(args[2]);
            return v.has_value() ? encode_bulk(*v) : encode_null(RespVersion::Resp2);
          }
          if (args.size() == 3 && upper(args[1]) == "OBJECT") {
            return encode_bulk("Value at:0x0 refcount:1 encoding:raw lru:0 lru_seconds_idle:0");
          }
          if (args.size() == 2 && upper(args[1]) == "LOADAOF") return encode_simple("OK");
          if (args.size() == 3 && upper(args[1]) == "SLEEP") {
            double secs = 0;
            try { secs = std::stod(args[2]); } catch (...) { return encode_error("Invalid debug sleep time"); }
            if (secs > 0) {
              std::this_thread::sleep_for(std::chrono::microseconds(static_cast<long long>(secs * 1000000)));
            }
            return encode_simple("OK");
          }
          if (args.size() == 3 && upper(args[1]) == "PROTOCOL") {
            const auto ptype = lower(args[2]);
            if (ptype == "bignum") {
              return std::string("(1234567999999999999999999999999999999\r\n");
            }
            if (ptype == "map") {
              return std::string("%3\r\n$10\r\nkeyone-key\r\n$8\r\nvalue-01\r\n$10\r\nkeytwo-key\r\n$8\r\nvalue-02\r\n$12\r\nkeythree-key\r\n$8\r\nvalue-03\r\n");
            }
            if (ptype == "set") {
              return std::string("~3\r\n$7\r\norange1\r\n$7\r\norange2\r\n$7\r\norange3\r\n");
            }
            if (ptype == "double") {
              return std::string(",3.141\r\n");
            }
            if (ptype == "null") {
              return std::string("_\r\n");
            }
            if (ptype == "verbatim") {
              return std::string("=29\r\ntxt:This is a verbatim\nstring\r\n");
            }
            if (ptype == "true") {
              return std::string("#t\r\n");
            }
            if (ptype == "false") {
              return std::string("#f\r\n");
            }
            if (ptype == "attrib") {
              return std::string("|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n$39\r\nSome real reply following the attribute\r\n");
            }
            return encode_error("Unknown protocol type");
          }
          return encode_error("unknown subcommand or wrong number of arguments for 'DEBUG'. Try DEBUG HELP.");
        }});

    t.emplace("MODULE", CommandSpec{"MODULE", -2, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          const auto sub = upper(args[1]);
          if (sub == "LOAD" && args.size() >= 3) {
            const std::string path = args[2];
            const std::string name = module_name_from_path(path);
            if (g_loaded_modules.find(name) != g_loaded_modules.end()) {
              return encode_error("Module already loaded");
            }
            void* handle = dlopen(path.c_str(), RTLD_NOW | RTLD_GLOBAL);
            if (!handle) return encode_error("Error loading the extension. Please check the server logs.");
            void* onload = dlsym(handle, "RedisModule_OnLoad");
            if (!onload) {
              dlclose(handle);
              return encode_error("Error loading the extension. Please check the server logs.");
            }
            using OnLoadFn = int (*)(void*, void*, int);
            g_current_loading_module = name;
            const int onload_rc = reinterpret_cast<OnLoadFn>(onload)(nullptr, nullptr, 0);
            g_current_loading_module.clear();
            if (onload_rc != 0) {
              dlclose(handle);
              return encode_error("Error loading the extension. Please check the server logs.");
            }
            g_loaded_modules.emplace(name, LoadedModule{handle, path});
            return encode_simple("OK");
          }
          if (sub == "UNLOAD" && args.size() == 3) {
            const std::string name = args[2];
            auto it = g_loaded_modules.find(name);
            if (it == g_loaded_modules.end()) return encode_error("no such module with that name");
            void* onunload = dlsym(it->second.handle, "RedisModule_OnUnload");
            if (onunload) {
              using OnUnloadFn = int (*)();
              (void)reinterpret_cast<OnUnloadFn>(onunload)();
            }
            std::vector<std::string> to_remove;
            for (const auto& [cmd_name, owner] : g_module_command_owner) {
              if (owner == name) to_remove.push_back(cmd_name);
            }
            for (const auto& cmd_name : to_remove) {
              g_module_command_owner.erase(cmd_name);
              g_module_commands.erase(cmd_name);
            }
            dlclose(it->second.handle);
            g_loaded_modules.erase(it);
            return encode_simple("OK");
          }
          if (sub == "LIST" && args.size() == 2) {
            std::vector<std::string> out;
            for (const auto& [name, mod] : g_loaded_modules) {
              out.push_back(encode_array({
                  encode_bulk("name"), encode_bulk(name),
                  encode_bulk("path"), encode_bulk(mod.path),
              }));
            }
            return encode_array(out);
          }
          return encode_error("unknown subcommand for MODULE");
        }});

    t.emplace("GET", CommandSpec{"GET", 2, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          if (store().is_wrongtype_for_string_noexpire(args[1])) return wrongtype_error_reply();
          if (store().lazy_expire_key(args[1])) {
            append_replication_event({"DEL", args[1]}, session);
          }
          auto v = store().get(args[1]);
          return v.has_value() ? encode_bulk(*v) : encode_null(RespVersion::Resp2);
        }});

    t.emplace("SET", CommandSpec{"SET", -3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          bool nx = false, xx = false, keepttl = false, get_old = false;
          std::optional<std::int64_t> expire_at;

          for (std::size_t i = 3; i < args.size(); ++i) {
            const std::string opt = upper(args[i]);
            if (opt == "NX") { nx = true; continue; }
            if (opt == "XX") { xx = true; continue; }
            if (opt == "KEEPTTL") { keepttl = true; continue; }
            if (opt == "GET") { get_old = true; continue; }
            if (opt == "EX" || opt == "PX" || opt == "EXAT" || opt == "PXAT") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              std::string err;
              auto exp = parse_expiry(opt, args[++i], err, "set");
              if (!exp.has_value()) return encode_error(err);
              expire_at = *exp;
              continue;
            }
            return encode_error("syntax error");
          }
          if (nx && xx) return encode_error("syntax error");

          return with_string_key(args[1], [&]() {
            const auto old = store().get(args[1]);
            const bool ok = store().set(args[1], args[2], nx, xx, expire_at, keepttl);
            if (get_old) return old.has_value() ? encode_bulk(*old) : encode_null(RespVersion::Resp2);
            if (!ok) return encode_null(RespVersion::Resp2);
            return encode_simple("OK");
          });
        }});

    t.emplace("SETEX", CommandSpec{"SETEX", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t sec = 0;
          if (!parse_i64(args[2], sec)) return encode_error("value is not an integer or out of range");
          if (sec <= 0) return encode_error("invalid expire time in setex");
          return with_string_key(args[1], [&]() {
            const auto exp = now_ms() + sec * 1000;
            const bool ok = store().set(args[1], args[3], false, false, exp, false);
            return ok ? encode_simple("OK") : encode_null(RespVersion::Resp2);
          });
        }});

    t.emplace("PSETEX", CommandSpec{"PSETEX", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t ms = 0;
          if (!parse_i64(args[2], ms)) return encode_error("value is not an integer or out of range");
          if (ms <= 0) return encode_error("invalid expire time in psetex");
          return with_string_key(args[1], [&]() {
            const auto exp = now_ms() + ms;
            const bool ok = store().set(args[1], args[3], false, false, exp, false);
            return ok ? encode_simple("OK") : encode_null(RespVersion::Resp2);
          });
        }});

    t.emplace("SETNX", CommandSpec{"SETNX", 3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_string_key(args[1], [&]() {
            const bool ok = store().set(args[1], args[2], true, false, std::nullopt, false);
            return encode_integer(ok ? 1 : 0);
          });
        }});

    t.emplace("MGET", CommandSpec{"MGET", -2, {"readonly"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::vector<std::string> out;
          for (std::size_t i = 1; i < args.size(); ++i) {
            auto v = store().get(args[i]);
            out.push_back(v.has_value() ? encode_bulk(*v) : encode_null(RespVersion::Resp2));
          }
          return encode_array(out);
        }});

    t.emplace("MSET", CommandSpec{"MSET", -3, {"write"}, 1, -1, 2,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (((args.size() - 1) % 2) != 0) return encode_error("wrong number of arguments for 'mset' command");
          for (std::size_t i = 1; i < args.size(); i += 2) {
            store().set(args[i], args[i + 1], false, false, std::nullopt, false);
          }
          return encode_simple("OK");
        }});

    t.emplace("MSETNX", CommandSpec{"MSETNX", -3, {"write"}, 1, -1, 2,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (((args.size() - 1) % 2) != 0) return encode_error("wrong number of arguments for 'msetnx' command");
          for (std::size_t i = 1; i < args.size(); i += 2) {
            if (store().get(args[i]).has_value()) return encode_integer(0);
          }
          for (std::size_t i = 1; i < args.size(); i += 2) {
            (void)store().set(args[i], args[i + 1], false, false, std::nullopt, false);
          }
          return encode_integer(1);
        }});

    t.emplace("GETDEL", CommandSpec{"GETDEL", 2, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_string_key(args[1], [&]() {
            auto v = store().getdel(args[1]);
            return v.has_value() ? encode_bulk(*v) : encode_null(RespVersion::Resp2);
          });
        }});

    t.emplace("GETSET", CommandSpec{"GETSET", 3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_string_key(args[1], [&]() {
            auto old = store().get(args[1]);
            (void)store().set(args[1], args[2], false, false, std::nullopt, false);
            return old.has_value() ? encode_bulk(*old) : encode_null(RespVersion::Resp2);
          });
        }});

    t.emplace("GETEX", CommandSpec{"GETEX", -2, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::optional<std::int64_t> exp;
          bool persist = false;
          if (args.size() > 2) {
            if (args.size() != 4 && !(args.size() == 3 && upper(args[2]) == "PERSIST")) return encode_error("syntax error");
            if (args.size() == 3) {
              persist = true;
            } else {
              std::string err;
              auto p = parse_expiry(args[2], args[3], err, "getex");
              if (!p.has_value()) return encode_error(err);
              exp = *p;
            }
          }
          return with_string_key(args[1], [&]() {
            auto v = store().getex(args[1], exp, persist);
            return v.has_value() ? encode_bulk(*v) : encode_null(RespVersion::Resp2);
          });
        }});

    t.emplace("APPEND", CommandSpec{"APPEND", 3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_string_key(args[1], [&]() { return encode_integer(store().append(args[1], args[2])); });
        }});

    t.emplace("STRLEN", CommandSpec{"STRLEN", 2, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_string_key(args[1], [&]() { return encode_integer(store().strlen(args[1])); });
        }});

    t.emplace("SETBIT", CommandSpec{"SETBIT", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t offset = 0;
          if (!parse_i64(args[2], offset) || offset < 0) {
            return encode_error("bit offset is not an integer or out of range");
          }
          std::int64_t bit = 0;
          if (!parse_i64(args[3], bit) || (bit != 0 && bit != 1)) {
            return encode_error("bit is not an integer or out of range");
          }
          return with_string_key(args[1], [&]() {
            bool ok = false;
            std::string err;
            const int prev = store().setbit(args[1], static_cast<std::uint64_t>(offset), static_cast<int>(bit), ok, err);
            if (!ok) {
              if (err == "WRONGTYPE") return wrongtype_error_reply();
              return encode_error(err.empty() ? "ERR" : err);
            }
            return encode_integer(prev);
          });
        }});

    t.emplace("GETBIT", CommandSpec{"GETBIT", 3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t offset = 0;
          if (!parse_i64(args[2], offset) || offset < 0) {
            return encode_error("bit offset is not an integer or out of range");
          }
          return with_string_key(args[1], [&]() {
            bool ok = false;
            std::string err;
            const int out = store().getbit(args[1], static_cast<std::uint64_t>(offset), ok, err);
            if (!ok) {
              if (err == "WRONGTYPE") return wrongtype_error_reply();
              return encode_error(err.empty() ? "ERR" : err);
            }
            return encode_integer(out);
          });
        }});

    t.emplace("SETRANGE", CommandSpec{"SETRANGE", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t offset = 0;
          if (!parse_i64(args[2], offset) || offset < 0) {
            return encode_error("offset is out of range");
          }
          return with_string_key(args[1], [&]() {
            bool ok = false;
            std::string err;
            const auto out = store().setrange(args[1], static_cast<std::size_t>(offset), args[3], ok, err);
            if (!ok) {
              if (err == "WRONGTYPE") return wrongtype_error_reply();
              return encode_error(err.empty() ? "ERR" : err);
            }
            return encode_integer(static_cast<long long>(out));
          });
        }});

    t.emplace("GETRANGE", CommandSpec{"GETRANGE", 4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t start = 0;
          std::int64_t stop = 0;
          if (!parse_i64(args[2], start) || !parse_i64(args[3], stop)) {
            return encode_error("value is not an integer or out of range");
          }
          return with_string_key(args[1], [&]() {
            bool ok = false;
            std::string err;
            const auto out = store().getrange(args[1], start, stop, ok, err);
            if (!ok) {
              if (err == "WRONGTYPE") return wrongtype_error_reply();
              return encode_error(err.empty() ? "ERR" : err);
            }
            return encode_bulk(out);
          });
        }});

    t.emplace("SUBSTR", CommandSpec{"SUBSTR", 4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool& close) {
          std::vector<std::string> proxy = {"GETRANGE", args[1], args[2], args[3]};
          return handle_command(proxy, session, close);
        }});

    t.emplace("LCS", CommandSpec{"LCS", -3, {"readonly"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          bool want_len = false;
          bool want_idx = false;
          bool with_match_len = false;
          std::int64_t min_match_len = 0;
          for (std::size_t i = 3; i < args.size(); ++i) {
            const auto opt = upper(args[i]);
            if (opt == "LEN") { want_len = true; continue; }
            if (opt == "IDX") { want_idx = true; continue; }
            if (opt == "WITHMATCHLEN") { with_match_len = true; continue; }
            if (opt == "MINMATCHLEN") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              if (!parse_i64(args[++i], min_match_len) || min_match_len < 0) {
                return encode_error("value is not an integer or out of range");
              }
              continue;
            }
            return encode_error("syntax error");
          }
          if (want_len && want_idx) return encode_error("syntax error");
          if (!want_idx && (with_match_len || min_match_len > 0)) return encode_error("syntax error");
          if (store().is_wrongtype_for_string(args[1]) || store().is_wrongtype_for_string(args[2])) {
            return wrongtype_error_reply();
          }
          const auto a = store().get(args[1]).value_or("");
          const auto b = store().get(args[2]).value_or("");
          const auto res = compute_lcs(a, b);
          if (want_len) return encode_integer(static_cast<long long>(res.lcs.size()));
          if (!want_idx) return encode_bulk(res.lcs);

          std::vector<std::string> matches;
          for (const auto& blk : res.blocks_desc) {
            if (min_match_len > 0 && blk.len < min_match_len) continue;
            std::vector<std::string> item = {
                encode_array({encode_integer(blk.a_start), encode_integer(blk.a_end)}),
                encode_array({encode_integer(blk.b_start), encode_integer(blk.b_end)}),
            };
            if (with_match_len) item.push_back(encode_integer(blk.len));
            matches.push_back(encode_array(item));
          }
          return encode_array({
              encode_bulk("matches"),
              encode_array(matches),
              encode_bulk("len"),
              encode_integer(static_cast<long long>(res.lcs.size())),
          });
        }});

    t.emplace("INCR", CommandSpec{"INCR", 2, {"write", "denyoom", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_string_key(args[1], [&]() {
            bool ok = false;
            std::string err;
            auto v = store().incrby(args[1], 1, ok, err);
            return ok ? encode_integer(v) : encode_error(err);
          });
        }});

    t.emplace("INCRBY", CommandSpec{"INCRBY", 3, {"write", "denyoom", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t by = 0;
          if (!parse_i64(args[2], by)) return encode_error("value is not an integer or out of range");
          return with_string_key(args[1], [&]() {
            bool ok = false;
            std::string err;
            auto v = store().incrby(args[1], by, ok, err);
            return ok ? encode_integer(v) : encode_error(err);
          });
        }});

    t.emplace("DECR", CommandSpec{"DECR", 2, {"write", "denyoom", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_string_key(args[1], [&]() {
            bool ok = false;
            std::string err;
            auto v = store().incrby(args[1], -1, ok, err);
            return ok ? encode_integer(v) : encode_error(err);
          });
        }});

    t.emplace("DECRBY", CommandSpec{"DECRBY", 3, {"write", "denyoom", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t by = 0;
          if (!parse_i64(args[2], by)) return encode_error("value is not an integer or out of range");
          return with_string_key(args[1], [&]() {
            bool ok = false;
            std::string err;
            auto v = store().incrby(args[1], -by, ok, err);
            return ok ? encode_integer(v) : encode_error(err);
          });
        }});

    t.emplace("INCRBYFLOAT", CommandSpec{"INCRBYFLOAT", 3, {"write", "denyoom", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          long double by = 0;
          if (!parse_f64(args[2], by)) return encode_error("value is not a valid float");
          return with_string_key(args[1], [&]() {
            bool ok = false;
            std::string err;
            auto v = store().incrbyfloat(args[1], by, ok, err);
            return ok ? encode_bulk(v) : encode_error(err);
          });
        }});

    t.emplace("DEL", CommandSpec{"DEL", -2, {"write"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::size_t n = 0;
          for (std::size_t i = 1; i < args.size(); ++i) n += store().del(args[i]) ? 1 : 0;
          return encode_integer(n);
        }});

    t.emplace("UNLINK", CommandSpec{"UNLINK", -2, {"write"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::size_t n = 0;
          for (std::size_t i = 1; i < args.size(); ++i) n += store().del(args[i]) ? 1 : 0;
          return encode_integer(n);
        }});

    t.emplace("EXISTS", CommandSpec{"EXISTS", -2, {"readonly"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::vector<std::string> keys(args.begin() + 1, args.end());
          return encode_integer(store().exists(keys));
        }});

    t.emplace("TYPE", CommandSpec{"TYPE", 2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) { return encode_simple(store().type_of(args[1])); }});

    t.emplace("TTL", CommandSpec{"TTL", 2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) { return encode_integer(store().ttl(args[1])); }});

    t.emplace("PTTL", CommandSpec{"PTTL", 2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) { return encode_integer(store().pttl(args[1])); }});

    t.emplace("EXPIRETIME", CommandSpec{"EXPIRETIME", 2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) { return encode_integer(store().expiretime(args[1])); }});

    t.emplace("PEXPIRETIME", CommandSpec{"PEXPIRETIME", 2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) { return encode_integer(store().pexpiretime(args[1])); }});

    t.emplace("EXPIRE", CommandSpec{"EXPIRE", -3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t sec = 0;
          if (!parse_i64(args[2], sec)) return encode_error("value is not an integer or out of range");
          if (sec > std::numeric_limits<std::int64_t>::max() / 1000 ||
              sec < std::numeric_limits<std::int64_t>::min() / 1000) {
            return encode_error("invalid expire time in 'expire' command");
          }
          const auto delta = sec * 1000;
          const auto base = now_ms();
          if ((delta > 0 && base > std::numeric_limits<std::int64_t>::max() - delta) ||
              (delta < 0 && base < std::numeric_limits<std::int64_t>::min() - delta)) {
            return encode_error("invalid expire time in 'expire' command");
          }
          bool nx = false, xx = false, gt = false, lt = false;
          for (std::size_t i = 3; i < args.size(); ++i) {
            const auto opt = upper(args[i]);
            if (opt == "NX") { nx = true; continue; }
            if (opt == "XX") { xx = true; continue; }
            if (opt == "GT") { gt = true; continue; }
            if (opt == "LT") { lt = true; continue; }
            return encode_error("Unsupported option " + args[i]);
          }
          if (gt && lt) return encode_error("GT and LT options at the same time are not compatible");
          if (nx && (xx || gt || lt)) return encode_error("NX and XX, GT or LT options at the same time are not compatible");
          const auto target = base + delta;
          const auto cur = store().pexpiretime(args[1]);
          if (cur == -2) return encode_integer(0);
          if (nx && cur != -1) return encode_integer(0);
          if (xx && cur < 0) return encode_integer(0);
          if (gt && (cur < 0 || target <= cur)) return encode_integer(0);
          if (lt && cur >= 0 && target >= cur) return encode_integer(0);
          return encode_integer(store().expire(args[1], target) ? 1 : 0);
        }});

    t.emplace("PEXPIRE", CommandSpec{"PEXPIRE", -3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t ms = 0;
          if (!parse_i64(args[2], ms)) return encode_error("value is not an integer or out of range");
          const auto base = now_ms();
          if ((ms > 0 && base > std::numeric_limits<std::int64_t>::max() - ms) ||
              (ms < 0 && base < std::numeric_limits<std::int64_t>::min() - ms)) {
            return encode_error("invalid expire time in 'pexpire' command");
          }
          bool nx = false, xx = false, gt = false, lt = false;
          for (std::size_t i = 3; i < args.size(); ++i) {
            const auto opt = upper(args[i]);
            if (opt == "NX") { nx = true; continue; }
            if (opt == "XX") { xx = true; continue; }
            if (opt == "GT") { gt = true; continue; }
            if (opt == "LT") { lt = true; continue; }
            return encode_error("Unsupported option " + args[i]);
          }
          if (gt && lt) return encode_error("GT and LT options at the same time are not compatible");
          if (nx && (xx || gt || lt)) return encode_error("NX and XX, GT or LT options at the same time are not compatible");
          const auto target = base + ms;
          const auto cur = store().pexpiretime(args[1]);
          if (cur == -2) return encode_integer(0);
          if (nx && cur != -1) return encode_integer(0);
          if (xx && cur < 0) return encode_integer(0);
          if (gt && (cur < 0 || target <= cur)) return encode_integer(0);
          if (lt && cur >= 0 && target >= cur) return encode_integer(0);
          return encode_integer(store().expire(args[1], target) ? 1 : 0);
        }});

    t.emplace("EXPIREAT", CommandSpec{"EXPIREAT", -3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t sec = 0;
          if (!parse_i64(args[2], sec)) return encode_error("value is not an integer or out of range");
          if (sec > std::numeric_limits<std::int64_t>::max() / 1000 ||
              sec < std::numeric_limits<std::int64_t>::min() / 1000) {
            return encode_error("invalid expire time in 'expireat' command");
          }
          bool nx = false, xx = false, gt = false, lt = false;
          for (std::size_t i = 3; i < args.size(); ++i) {
            const auto opt = upper(args[i]);
            if (opt == "NX") { nx = true; continue; }
            if (opt == "XX") { xx = true; continue; }
            if (opt == "GT") { gt = true; continue; }
            if (opt == "LT") { lt = true; continue; }
            return encode_error("Unsupported option " + args[i]);
          }
          if (gt && lt) return encode_error("GT and LT options at the same time are not compatible");
          if (nx && (xx || gt || lt)) return encode_error("NX and XX, GT or LT options at the same time are not compatible");
          const auto target = sec * 1000;
          const auto cur = store().pexpiretime(args[1]);
          if (cur == -2) return encode_integer(0);
          if (nx && cur != -1) return encode_integer(0);
          if (xx && cur < 0) return encode_integer(0);
          if (gt && (cur < 0 || target <= cur)) return encode_integer(0);
          if (lt && cur >= 0 && target >= cur) return encode_integer(0);
          return encode_integer(store().expire(args[1], target) ? 1 : 0);
        }});

    t.emplace("PEXPIREAT", CommandSpec{"PEXPIREAT", -3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t ms = 0;
          if (!parse_i64(args[2], ms)) return encode_error("value is not an integer or out of range");
          bool nx = false, xx = false, gt = false, lt = false;
          for (std::size_t i = 3; i < args.size(); ++i) {
            const auto opt = upper(args[i]);
            if (opt == "NX") { nx = true; continue; }
            if (opt == "XX") { xx = true; continue; }
            if (opt == "GT") { gt = true; continue; }
            if (opt == "LT") { lt = true; continue; }
            return encode_error("Unsupported option " + args[i]);
          }
          if (gt && lt) return encode_error("GT and LT options at the same time are not compatible");
          if (nx && (xx || gt || lt)) return encode_error("NX and XX, GT or LT options at the same time are not compatible");
          const auto cur = store().pexpiretime(args[1]);
          if (cur == -2) return encode_integer(0);
          if (nx && cur != -1) return encode_integer(0);
          if (xx && cur < 0) return encode_integer(0);
          if (gt && (cur < 0 || ms <= cur)) return encode_integer(0);
          if (lt && cur >= 0 && ms >= cur) return encode_integer(0);
          return encode_integer(store().expire(args[1], ms) ? 1 : 0);
        }});

    t.emplace("PERSIST", CommandSpec{"PERSIST", 2, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) { return encode_integer(store().persist(args[1]) ? 1 : 0); }});

    t.emplace("SELECT", CommandSpec{"SELECT", 2, {"loading"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::int64_t db = 0;
          if (!parse_i64(args[1], db)) return encode_error("value is not an integer or out of range");
          if (!store().select_db(static_cast<int>(db))) return encode_error("DB index is out of range");
          session.db_index = static_cast<int>(db);
          return encode_simple("OK");
        }});

    t.emplace("SWAPDB", CommandSpec{"SWAPDB", 3, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t a = 0, b = 0;
          if (!parse_i64(args[1], a) || !parse_i64(args[2], b)) return encode_error("value is not an integer or out of range");
          if (!store().swapdb(static_cast<int>(a), static_cast<int>(b))) return encode_error("DB index is out of range");
          return encode_simple("OK");
        }});

    t.emplace("DBSIZE", CommandSpec{"DBSIZE", 1, {"readonly", "fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          return encode_integer(static_cast<long long>(store().dbsize()));
        }});

    t.emplace("KEYS", CommandSpec{"KEYS", 2, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          auto keys = store().keys(args[1]);
          std::sort(keys.begin(), keys.end());
          std::vector<std::string> out;
          for (const auto& k : keys) out.push_back(encode_bulk(k));
          return encode_array(out);
        }});

    t.emplace("RANDOMKEY", CommandSpec{"RANDOMKEY", 1, {"readonly", "fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) {
          append_lazy_expire_dels(session);
          const auto k = store().randomkey();
          return k.has_value() ? encode_bulk(*k) : encode_null(RespVersion::Resp2);
        }});

    t.emplace("SCAN", CommandSpec{"SCAN", -2, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::uint64_t cursor = 0;
          try { cursor = static_cast<std::uint64_t>(std::stoull(args[1])); } catch (...) { return encode_error("invalid cursor"); }
          std::string match_pattern = "*";
          std::size_t count = 10;
          std::string type_filter;
          for (std::size_t i = 2; i < args.size(); i += 2) {
            if (i + 1 >= args.size()) return encode_error("syntax error");
            auto opt = upper(args[i]);
            if (opt == "COUNT") {
              try { count = static_cast<std::size_t>(std::stoull(args[i + 1])); }
              catch (...) { return encode_error("value is not an integer or out of range"); }
            } else if (opt == "MATCH") {
              match_pattern = args[i + 1];
            } else if (opt == "TYPE") {
              type_filter = lower(args[i + 1]);
            } else {
              return encode_error("syntax error");
            }
          }
          append_lazy_expire_dels(session);
          auto [next, keys] = store().scan(cursor, count, match_pattern, type_filter);
          std::vector<std::string> inner;
          inner.reserve(keys.size());
          for (const auto& k : keys) inner.push_back(encode_bulk(k));
          return encode_array({encode_bulk(std::to_string(next)), encode_array(inner)});
        }});

    t.emplace("SORT", CommandSpec{"SORT", -2, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::string store_key;
          bool desc = false;
          bool alpha = false;
          std::string by_pattern;
          std::vector<std::string> get_patterns;
          std::int64_t limit_offset = 0, limit_count = -1;
          for (std::size_t i = 2; i < args.size(); ++i) {
            const auto opt = upper(args[i]);
            if (opt == "STORE" && i + 1 < args.size()) {
              store_key = args[++i];
            } else if (opt == "DESC") {
              desc = true;
            } else if (opt == "ASC") {
              desc = false;
            } else if (opt == "ALPHA") {
              alpha = true;
            } else if (opt == "BY" && i + 1 < args.size()) {
              by_pattern = args[++i];
            } else if (opt == "GET" && i + 1 < args.size()) {
              get_patterns.push_back(args[++i]);
            } else if (opt == "LIMIT" && i + 2 < args.size()) {
              if (!parse_i64(args[i+1], limit_offset) || !parse_i64(args[i+2], limit_count))
                return encode_error("value is not an integer or out of range");
              i += 2;
            } else {
              return encode_error("syntax error");
            }
          }
          const auto type = store().type_of(args[1]);
          std::vector<std::string> vals;
          if (type == "none") {
            vals.clear();
          } else if (type == "list") {
            bool wrongtype = false;
            vals = store().lrange(args[1], 0, -1, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
          } else if (type == "set") {
            bool wrongtype = false;
            vals = store().smembers(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
          } else if (type == "zset") {
            bool wrongtype = false;
            auto pairs = store().zrange(args[1], 0, -1, false, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            for (const auto& p : pairs) vals.push_back(p.first);
          } else {
            return wrongtype_error_reply();
          }
          // BY _ means "no sort" (natural/alphabetical order from iteration)
          if (by_pattern != "_") {
            if (!by_pattern.empty()) {
              // Sort by external key pattern
              // Replace * in pattern with val to get the sort key
              auto get_by_key = [&](const std::string& val) -> std::string {
                std::string key = by_pattern;
                auto pos = key.find('*');
                if (pos != std::string::npos) key.replace(pos, 1, val);
                auto v = store().get(key);
                return v.value_or("0");
              };
              if (alpha) {
                std::sort(vals.begin(), vals.end(), [&](const std::string& a, const std::string& b) {
                  const auto va = get_by_key(a), vb = get_by_key(b);
                  if (desc) return va > vb;
                  return va < vb;
                });
              } else {
                auto to_num = [](const std::string& s) -> double {
                  try { return std::stod(s); } catch (...) { return 0; }
                };
                std::sort(vals.begin(), vals.end(), [&](const std::string& a, const std::string& b) {
                  const auto va = to_num(get_by_key(a)), vb = to_num(get_by_key(b));
                  if (desc) return va > vb;
                  return va < vb;
                });
              }
            } else if (alpha) {
              // Alphabetical sort
              if (desc)
                std::sort(vals.begin(), vals.end(), std::greater<std::string>());
              else
                std::sort(vals.begin(), vals.end());
            } else {
              // Numeric sort (default)
              auto to_num = [](const std::string& s) -> double {
                try { return std::stod(s); } catch (...) { return 0; }
              };
              if (desc)
                std::sort(vals.begin(), vals.end(), [&](const std::string& a, const std::string& b) { return to_num(a) > to_num(b); });
              else
                std::sort(vals.begin(), vals.end(), [&](const std::string& a, const std::string& b) { return to_num(a) < to_num(b); });
            }
          } else {
            // BY _ : alphabetical/natural order
            std::sort(vals.begin(), vals.end());
          }
          // Apply LIMIT
          if (limit_count >= 0) {
            if (static_cast<std::size_t>(limit_offset) >= vals.size()) {
              vals.clear();
            } else {
              auto end = std::min(vals.size(), static_cast<std::size_t>(limit_offset + limit_count));
              vals = std::vector<std::string>(vals.begin() + limit_offset, vals.begin() + static_cast<std::ptrdiff_t>(end));
            }
          }
          if (!store_key.empty()) {
            store().del(store_key);
            if (!vals.empty()) {
              bool wrongtype = false;
              (void)store().rpush(store_key, vals, wrongtype);
            }
            return encode_integer(static_cast<long long>(vals.size()));
          }
          // Build output
          std::vector<std::string> out;
          if (get_patterns.empty()) {
            out.reserve(vals.size());
            for (const auto& v : vals) out.push_back(encode_bulk(v));
          } else {
            for (const auto& v : vals) {
              for (const auto& pat : get_patterns) {
                if (pat == "#") {
                  out.push_back(encode_bulk(v));
                } else {
                  std::string key = pat;
                  auto pos = key.find('*');
                  if (pos != std::string::npos) key.replace(pos, 1, v);
                  auto got = store().get(key);
                  if (got.has_value()) {
                    out.push_back(encode_bulk(*got));
                  } else {
                    out.push_back(encode_null(RespVersion::Resp2));
                  }
                }
              }
            }
          }
          return encode_array(out);
        }});

    t.emplace("RENAME", CommandSpec{"RENAME", 3, {"write"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (!store().rename(args[1], args[2], false)) return encode_error("no such key");
          return encode_simple("OK");
        }});

    t.emplace("RENAMENX", CommandSpec{"RENAMENX", 3, {"write"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return encode_integer(store().rename(args[1], args[2], true) ? 1 : 0);
        }});

    t.emplace("COPY", CommandSpec{"COPY", -3, {"write"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          bool replace = false;
          int target_db = store().current_db();
          for (std::size_t i = 3; i < args.size(); ++i) {
            const std::string opt = upper(args[i]);
            if (opt == "REPLACE") {
              replace = true;
              continue;
            }
            if (opt == "DB") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              std::int64_t db = 0;
              if (!parse_i64(args[++i], db) || db < 0) return encode_error("value is not an integer or out of range");
              target_db = static_cast<int>(db);
              continue;
            }
            return encode_error("syntax error");
          }
          return encode_integer(store().copy_key(args[1], args[2], target_db, replace));
        }});

    t.emplace("MOVE", CommandSpec{"MOVE", 3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t db = 0;
          if (!parse_i64(args[2], db)) return encode_error("value is not an integer or out of range");
          if (db < 0 || db >= 16) return encode_error("DB index is out of range");
          return encode_integer(store().move_key(args[1], static_cast<int>(db)));
        }});

    t.emplace("DUMP", CommandSpec{"DUMP", 2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (store().type_of(args[1]) == "none") return encode_null(RespVersion::Resp2);
          auto payload = rdb_dump_key(args[1]);
          if (payload.empty()) return encode_null(RespVersion::Resp2);
          return encode_bulk(payload);
        }});

    t.emplace("RESTORE", CommandSpec{"RESTORE", -4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t ttl_ms = 0;
          if (!parse_i64(args[2], ttl_ms) || ttl_ms < 0) {
            return encode_error("value is not an integer or out of range");
          }

          bool replace = false;
          bool absttl = false;
          for (std::size_t i = 4; i < args.size(); ++i) {
            const std::string opt = upper(args[i]);
            if (opt == "REPLACE") {
              replace = true;
              continue;
            }
            if (opt == "ABSTTL") {
              absttl = true;
              continue;
            }
            if (opt == "IDLETIME" || opt == "FREQ") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              ++i;
              continue;
            }
            return encode_error("syntax error");
          }

          std::string err;
          if (!rdb_restore_key(args[1], args[3], ttl_ms, replace, absttl, err)) {
            if (err.find("Target key") != std::string::npos) {
              return std::string("-BUSYKEY ") + err + "\r\n";
            }
            return encode_error(err);
          }
          return encode_simple("OK");
        }});

    t.emplace("MIGRATE", CommandSpec{"MIGRATE", -6, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          const std::string host = args[1];
          std::int64_t port = 0;
          std::int64_t db = 0;
          std::int64_t timeout = 0;
          if (!parse_i64(args[2], port) || port <= 0 || port > 65535) {
            return encode_error("invalid port");
          }
          const std::string key = args[3];
          if (!parse_i64(args[4], db) || db < 0) {
            return encode_error("value is not an integer or out of range");
          }
          if (!parse_i64(args[5], timeout) || timeout < 0) {
            return encode_error("value is not an integer or out of range");
          }
          bool copy = false;
          bool replace = false;
          for (std::size_t i = 6; i < args.size(); ++i) {
            const auto opt = upper(args[i]);
            if (opt == "COPY") { copy = true; continue; }
            if (opt == "REPLACE") { replace = true; continue; }
            if (opt == "AUTH" || opt == "AUTH2" || opt == "KEYS") {
              return encode_error("Unsupported MIGRATE option");
            }
            return encode_error("syntax error");
          }

          if (store().type_of(key) == "none") return encode_simple("NOKEY");
          const std::string payload = rdb_dump_key(key);
          if (payload.empty()) return encode_simple("NOKEY");
          std::int64_t pttl = store().pttl(key);
          if (pttl < 0) pttl = 0;

          const int fd = socket(AF_INET, SOCK_STREAM, 0);
          if (fd < 0) return std::string("-IOERR error or timeout connecting to the client\r\n");
          timeval tv{};
          tv.tv_sec = static_cast<long>(timeout / 1000);
          tv.tv_usec = static_cast<suseconds_t>((timeout % 1000) * 1000);
          if (timeout > 0) {
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
          }
          sockaddr_in sa{};
          sa.sin_family = AF_INET;
          sa.sin_port = htons(static_cast<uint16_t>(port));
          if (inet_pton(AF_INET, host.c_str(), &sa.sin_addr) != 1) {
            close(fd);
            return std::string("-IOERR error or timeout connecting to the client\r\n");
          }
          if (connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
            close(fd);
            return std::string("-IOERR error or timeout connecting to the client\r\n");
          }

          std::string payload_reply;
          bool is_err = false;
          std::vector<std::string> select_args = {"SELECT", std::to_string(db)};
          const auto select_wire = encode_command_resp(select_args);
          if (!write_all(fd, select_wire.data(), select_wire.size()) || !read_resp_reply(fd, payload_reply, is_err) || is_err) {
            close(fd);
            return std::string("-IOERR error or timeout writing to target instance\r\n");
          }

          std::vector<std::string> restore_args = {"RESTORE", key, std::to_string(pttl), payload};
          if (replace) restore_args.push_back("REPLACE");
          std::vector<std::string> asking_args = {"ASKING"};
          const auto asking_wire = encode_command_resp(asking_args);
          if (!write_all(fd, asking_wire.data(), asking_wire.size()) || !read_resp_reply(fd, payload_reply, is_err) || is_err) {
            close(fd);
            return std::string("-IOERR error or timeout writing to target instance\r\n");
          }
          const auto restore_wire = encode_command_resp(restore_args);
          if (!write_all(fd, restore_wire.data(), restore_wire.size()) || !read_resp_reply(fd, payload_reply, is_err)) {
            close(fd);
            return std::string("-IOERR error or timeout writing to target instance\r\n");
          }
          close(fd);
          if (is_err) {
            return std::string("-IOERR ") + payload_reply + "\r\n";
          }
          if (!copy) {
            store().del(key);
          }
          return encode_simple("OK");
        }});

    t.emplace("FLUSHALL", CommandSpec{"FLUSHALL", 1, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          store().flushall();
          return encode_simple("OK");
        }});

    t.emplace("FLUSHDB", CommandSpec{"FLUSHDB", 1, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          store().flushdb();
          return encode_simple("OK");
        }});

    t.emplace("FUNCTION", CommandSpec{"FUNCTION", -2, {"loading", "stale"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          const auto sub = upper(args[1]);
          if (sub == "FLUSH") {
            g_functions.clear();
            g_function_libraries.clear();
            return encode_simple("OK");
          }
          if (sub == "LOAD" && args.size() >= 3) {
            bool replace = false;
            std::string payload;
            if (args.size() == 3) {
              payload = args[2];
            } else if (args.size() == 4 && upper(args[2]) == "REPLACE") {
              replace = true;
              payload = args[3];
            } else {
              return encode_error("syntax error");
            }
            std::string lib, fn, body;
            bool no_writes = false;
            if (!parse_function_load_payload(payload, lib, fn, body, no_writes)) {
              return encode_error("ERR Error compiling function");
            }
            if (!replace && g_function_libraries.find(lib) != g_function_libraries.end()) {
              return encode_error("Library already exists");
            }
            if (replace) {
              auto it = g_function_libraries.find(lib);
              if (it != g_function_libraries.end()) {
                for (const auto& f : it->second) g_functions.erase(f);
                g_function_libraries.erase(it);
              }
            }
            g_functions[fn] = FunctionDef{lib, body, no_writes};
            g_function_libraries[lib] = {fn};
            return encode_bulk(lib);
          }
          if (sub == "DELETE" && args.size() == 3) {
            auto it = g_function_libraries.find(args[2]);
            if (it == g_function_libraries.end()) return encode_error("Library not found");
            for (const auto& f : it->second) g_functions.erase(f);
            g_function_libraries.erase(it);
            return encode_simple("OK");
          }
          if (sub == "LIST") {
            std::vector<std::string> out;
            for (const auto& [lib, _] : g_function_libraries) out.push_back(encode_bulk(lib));
            return encode_array(out);
          }
          if (sub == "STATS") {
            return encode_array({encode_bulk("running_script"), encode_null(RespVersion::Resp2)});
          }
          if (sub == "KILL") {
            g_script_busy.store(false);
            g_script_kill_requested.store(true);
            g_busy_script_session = nullptr;
            return encode_simple("OK");
          }
          return encode_error("unknown subcommand for FUNCTION");
        }});

    t.emplace("FCALL", CommandSpec{"FCALL", -3, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          auto it = g_functions.find(args[1]);
          if (it == g_functions.end()) return encode_error("Function not found");
          std::int64_t numkeys = 0;
          if (!parse_i64(args[2], numkeys)) return encode_error("value is not an integer or out of range");
          if (numkeys < 0) return encode_error("Number of keys can't be negative");
          if (args.size() < static_cast<std::size_t>(3 + numkeys)) return encode_error("Number of keys can't be greater than number of args");
          std::vector<std::string> keys;
          std::vector<std::string> argv;
          for (std::int64_t i = 0; i < numkeys; ++i) keys.push_back(args[3 + i]);
          for (std::size_t i = static_cast<std::size_t>(3 + numkeys); i < args.size(); ++i) argv.push_back(args[i]);
          g_script_client_resp_version = session.resp_version;
          g_script_current_session = &session;
          const auto out = eval_miniscript(it->second.body, keys, argv);
          g_script_current_session = nullptr;
          return out;
        }});

    t.emplace("FCALL_RO", CommandSpec{"FCALL_RO", -3, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool& close) {
          std::vector<std::string> proxy = args;
          proxy[0] = "FCALL";
          g_script_readonly_context = true;
          const auto out = handle_command(proxy, session, close);
          g_script_readonly_context = false;
          return out;
        }});

    t.emplace("OBJECT", CommandSpec{"OBJECT", -2, {"readonly"}, 2, 2, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&){
          if (args.size() != 3) return encode_error("syntax error");
          const auto sub = upper(args[1]);
          const auto t = store().type_of(args[2]);
          if (sub == "REFCOUNT") {
            if (t == "none") return encode_null(RespVersion::Resp2);
            return encode_integer(1);
          }
          if (sub == "ENCODING") {
            auto enc = store().object_encoding(args[2]);
            return enc.has_value() ? encode_bulk(*enc) : encode_null(RespVersion::Resp2);
          }
          return encode_error("syntax error");
        }});

    t.emplace("HSET", CommandSpec{"HSET", -4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (((args.size() - 2) % 2) != 0) return encode_error("wrong number of arguments for 'hset' command");
          return with_hash_key(args[1], [&]() {
            std::vector<std::pair<std::string, std::string>> fields;
            for (std::size_t i = 2; i < args.size(); i += 2) fields.emplace_back(args[i], args[i + 1]);
            bool wrongtype = false;
            const auto added = store().hset(args[1], fields, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(added);
          });
        }});

    t.emplace("HGET", CommandSpec{"HGET", 3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            auto v = store().hget(args[1], args[2], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return v.has_value() ? encode_bulk(*v) : encode_null(RespVersion::Resp2);
          });
        }});

    t.emplace("HMGET", CommandSpec{"HMGET", -3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            // Check type first with hlen
            (void)store().hlen(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            out.reserve(args.size() - 2);
            for (std::size_t i = 2; i < args.size(); ++i) {
              auto v = store().hget(args[1], args[i], wrongtype);
              out.push_back(v.has_value() ? encode_bulk(*v) : encode_null(RespVersion::Resp2));
            }
            return encode_array(out);
          });
        }});

    t.emplace("HMSET", CommandSpec{"HMSET", -4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (((args.size() - 2) % 2) != 0) return encode_error("wrong number of arguments for 'hmset' command");
          return with_hash_key(args[1], [&]() {
            std::vector<std::pair<std::string, std::string>> fields;
            for (std::size_t i = 2; i < args.size(); i += 2) fields.emplace_back(args[i], args[i + 1]);
            bool wrongtype = false;
            store().hset(args[1], fields, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_simple("OK");
          });
        }});

    t.emplace("HSETNX", CommandSpec{"HSETNX", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            auto existing = store().hget(args[1], args[2], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            if (existing.has_value()) return encode_integer(0);
            std::vector<std::pair<std::string, std::string>> fields{{args[2], args[3]}};
            store().hset(args[1], fields, wrongtype);
            return encode_integer(1);
          });
        }});

    t.emplace("HVALS", CommandSpec{"HVALS", 2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            const auto fields = store().hgetall(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            out.reserve(fields.size());
            for (const auto& [f, v] : fields) {
              out.push_back(encode_bulk(v));
            }
            return encode_array(out);
          });
        }});

    t.emplace("HKEYS", CommandSpec{"HKEYS", 2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            const auto fields = store().hgetall(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            out.reserve(fields.size());
            for (const auto& [f, v] : fields) {
              out.push_back(encode_bulk(f));
            }
            return encode_array(out);
          });
        }});

    t.emplace("HINCRBY", CommandSpec{"HINCRBY", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            std::int64_t increment = 0;
            if (!parse_i64(args[3], increment)) return encode_error("value is not an integer or out of range");
            bool wrongtype = false;
            auto existing = store().hget(args[1], args[2], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::int64_t current = 0;
            if (existing.has_value()) {
              if (!parse_i64(*existing, current)) return encode_error("hash value is not an integer");
            }
            current += increment;
            std::vector<std::pair<std::string, std::string>> fields{{args[2], std::to_string(current)}};
            store().hset(args[1], fields, wrongtype);
            return encode_integer(current);
          });
        }});

    t.emplace("HINCRBYFLOAT", CommandSpec{"HINCRBYFLOAT", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            long double increment = 0;
            if (!parse_f64(args[3], increment)) return encode_error("value is not a valid float");
            bool wrongtype = false;
            auto existing = store().hget(args[1], args[2], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            long double current = 0;
            if (existing.has_value()) {
              if (!parse_f64(*existing, current)) return encode_error("hash value is not a valid float");
            }
            current += increment;
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%.17Lg", current);
            std::string val(buf);
            // Remove trailing zeros after decimal point
            if (val.find('.') != std::string::npos) {
              auto pos = val.find_last_not_of('0');
              if (pos != std::string::npos && val[pos] == '.') pos--;
              val = val.substr(0, pos + 1);
            }
            std::vector<std::pair<std::string, std::string>> fields{{args[2], val}};
            store().hset(args[1], fields, wrongtype);
            return encode_bulk(val);
          });
        }});

    t.emplace("HDEL", CommandSpec{"HDEL", -3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            std::vector<std::string> fields(args.begin() + 2, args.end());
            bool wrongtype = false;
            const auto removed = store().hdel(args[1], fields, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(removed);
          });
        }});

    t.emplace("HLEN", CommandSpec{"HLEN", 2, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            const auto n = store().hlen(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(n);
          });
        }});

    t.emplace("HEXISTS", CommandSpec{"HEXISTS", 3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            const auto ok = store().hexists(args[1], args[2], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(ok ? 1 : 0);
          });
        }});

    t.emplace("HGETALL", CommandSpec{"HGETALL", 2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            const auto fields = store().hgetall(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            out.reserve(fields.size() * 2);
            for (const auto& [f, v] : fields) {
              out.push_back(encode_bulk(f));
              out.push_back(encode_bulk(v));
            }
            return encode_array(out);
          });
        }});

    t.emplace("HSCAN", CommandSpec{"HSCAN", -3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::uint64_t cursor = 0;
          try {
            cursor = static_cast<std::uint64_t>(std::stoull(args[2]));
          } catch (...) {
            return encode_error("invalid cursor");
          }
          std::size_t count = 10;
          for (std::size_t i = 3; i < args.size(); ++i) {
            if (upper(args[i]) == "COUNT" && i + 1 < args.size()) {
              try { count = static_cast<std::size_t>(std::stoull(args[++i])); } catch (...) { return encode_error("value is not an integer or out of range"); }
              continue;
            }
            return encode_error("syntax error");
          }
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            const auto [next, fields] = store().hscan(args[1], cursor, count, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> inner;
            inner.reserve(fields.size() * 2);
            for (const auto& [f, v] : fields) {
              inner.push_back(encode_bulk(f));
              inner.push_back(encode_bulk(v));
            }
            return encode_array({encode_bulk(std::to_string(next)), encode_array(inner)});
          });
        }});

    t.emplace("LPUSH", CommandSpec{"LPUSH", -3, {"write", "denyoom"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          return with_list_key(args[1], [&]() {
            std::vector<std::string> vals(args.begin() + 2, args.end());
            bool wrongtype = false;
            auto n = store().lpush(args[1], vals, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            notify_key_written(args[1], session.db_index);
            return encode_integer(n);
          });
        }});

    t.emplace("RPUSH", CommandSpec{"RPUSH", -3, {"write", "denyoom"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          return with_list_key(args[1], [&]() {
            std::vector<std::string> vals(args.begin() + 2, args.end());
            bool wrongtype = false;
            auto n = store().rpush(args[1], vals, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            notify_key_written(args[1], session.db_index);
            return encode_integer(n);
          });
        }});

    t.emplace("LPOP", CommandSpec{"LPOP", 2, {"write", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_list_key(args[1], [&]() {
            bool wrongtype = false;
            auto v = store().lpop(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return v.has_value() ? encode_bulk(*v) : encode_null(RespVersion::Resp2);
          });
        }});

    t.emplace("RPOP", CommandSpec{"RPOP", 2, {"write", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_list_key(args[1], [&]() {
            bool wrongtype = false;
            auto v = store().rpop(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return v.has_value() ? encode_bulk(*v) : encode_null(RespVersion::Resp2);
          });
        }});

    t.emplace("LLEN", CommandSpec{"LLEN", 2, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_list_key(args[1], [&]() {
            bool wrongtype = false;
            auto n = store().llen(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(n);
          });
        }});

    t.emplace("LRANGE", CommandSpec{"LRANGE", 4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t start = 0, stop = 0;
          if (!parse_i64(args[2], start) || !parse_i64(args[3], stop)) {
            return encode_error("value is not an integer or out of range");
          }
          return with_list_key(args[1], [&]() {
            bool wrongtype = false;
            auto vals = store().lrange(args[1], start, stop, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            out.reserve(vals.size());
            for (const auto& v : vals) out.push_back(encode_bulk(v));
            return encode_array(out);
          });
        }});

    t.emplace("BLPOP", CommandSpec{"BLPOP", -3, {"write", "blocking"}, 1, -2, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          long double timeout_sec = 0;
          if (!parse_f64(args.back(), timeout_sec) || timeout_sec < 0) {
            return encode_error("timeout is not a float or out of range");
          }
          // Try immediate pop first
          for (std::size_t i = 1; i + 1 < args.size(); ++i) {
            const auto& key = args[i];
            if (store().is_wrongtype_for_list(key)) return wrongtype_error_reply();
            bool wrongtype = false;
            auto v = store().lpop(key, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            if (v.has_value()) return encode_array({encode_bulk(key), encode_bulk(*v)});
          }
          // In Lua script or EXEC context: return nil immediately (no blocking)
          if (g_in_lua_script || g_executing_exec) return encode_null_array();
          // No data available — block the client
          session.blocked.type = BlockType::List;
          session.blocked.pop_left = true;
          session.blocked.keys.clear();
          for (std::size_t i = 1; i + 1 < args.size(); ++i) {
            session.blocked.keys.push_back(args[i]);
            register_key_waiter(args[i], session.fd);
          }
          session.blocked.original_args = args;
          if (timeout_sec > 0) {
            session.blocked.deadline_ms = now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L);
          } else {
            session.blocked.deadline_ms = 0; // infinite
          }
          return std::string(); // empty = don't send reply yet
        }});

    t.emplace("BRPOP", CommandSpec{"BRPOP", -3, {"write", "blocking"}, 1, -2, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          long double timeout_sec = 0;
          if (!parse_f64(args.back(), timeout_sec) || timeout_sec < 0) {
            return encode_error("timeout is not a float or out of range");
          }
          for (std::size_t i = 1; i + 1 < args.size(); ++i) {
            const auto& key = args[i];
            if (store().is_wrongtype_for_list(key)) return wrongtype_error_reply();
            bool wrongtype = false;
            auto v = store().rpop(key, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            if (v.has_value()) return encode_array({encode_bulk(key), encode_bulk(*v)});
          }
          // In Lua script or EXEC context: return nil immediately (no blocking)
          if (g_in_lua_script || g_executing_exec) return encode_null_array();
          session.blocked.type = BlockType::List;
          session.blocked.pop_left = false;
          session.blocked.keys.clear();
          for (std::size_t i = 1; i + 1 < args.size(); ++i) {
            session.blocked.keys.push_back(args[i]);
            register_key_waiter(args[i], session.fd);
          }
          session.blocked.original_args = args;
          if (timeout_sec > 0) {
            session.blocked.deadline_ms = now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L);
          } else {
            session.blocked.deadline_ms = 0;
          }
          return std::string();
        }});

    t.emplace("LMOVE", CommandSpec{"LMOVE", 5, {"write"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          const std::string src = args[1];
          const std::string dst = args[2];
          const std::string where_from = upper(args[3]);
          const std::string where_to = upper(args[4]);
          if ((where_from != "LEFT" && where_from != "RIGHT") || (where_to != "LEFT" && where_to != "RIGHT")) {
            return encode_error("syntax error");
          }
          if (store().is_wrongtype_for_list(src) || store().is_wrongtype_for_list(dst)) return wrongtype_error_reply();

          bool wrongtype = false;
          std::optional<std::string> popped;
          if (where_from == "LEFT") popped = store().lpop(src, wrongtype);
          else popped = store().rpop(src, wrongtype);
          if (wrongtype) return wrongtype_error_reply();
          if (!popped.has_value()) return encode_null(RespVersion::Resp2);

          std::vector<std::string> one{*popped};
          if (where_to == "LEFT") (void)store().lpush(dst, one, wrongtype);
          else (void)store().rpush(dst, one, wrongtype);
          if (wrongtype) return wrongtype_error_reply();
          return encode_bulk(*popped);
        }});

    t.emplace("BLMOVE", CommandSpec{"BLMOVE", 6, {"write", "blocking"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          long double timeout_sec = 0;
          if (!parse_f64(args[5], timeout_sec) || timeout_sec < 0) {
            return encode_error("timeout is not a float or out of range");
          }
          const std::string src = args[1];
          const std::string dst = args[2];
          const std::string where_from = upper(args[3]);
          const std::string where_to = upper(args[4]);
          if ((where_from != "LEFT" && where_from != "RIGHT") || (where_to != "LEFT" && where_to != "RIGHT")) {
            return encode_error("syntax error");
          }
          if (store().is_wrongtype_for_list(src) || store().is_wrongtype_for_list(dst)) return wrongtype_error_reply();

          bool wrongtype = false;
          std::optional<std::string> popped;
          if (where_from == "LEFT") popped = store().lpop(src, wrongtype);
          else popped = store().rpop(src, wrongtype);
          if (wrongtype) return wrongtype_error_reply();
          if (popped.has_value()) {
            std::vector<std::string> one{*popped};
            if (where_to == "LEFT") (void)store().lpush(dst, one, wrongtype);
            else (void)store().rpush(dst, one, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_bulk(*popped);
          }
          // In Lua script or EXEC context: return nil immediately (no blocking)
          if (g_in_lua_script || g_executing_exec) return encode_null(RespVersion::Resp2);
          session.blocked.type = BlockType::List;
          session.blocked.pop_left = (where_from == "LEFT");
          session.blocked.keys = {src};
          session.blocked.original_args = args;
          if (timeout_sec > 0) {
            session.blocked.deadline_ms = now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L);
          } else {
            session.blocked.deadline_ms = 0;
          }
          return std::string();
        }});

    t.emplace("BRPOPLPUSH", CommandSpec{"BRPOPLPUSH", 4, {"write", "blocking"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::vector<std::string> proxy = {"BLMOVE", args[1], args[2], "RIGHT", "LEFT", args[3]};
          bool close = false;
          return handle_command(proxy, session, close);
        }});

    t.emplace("BZPOPMIN", CommandSpec{"BZPOPMIN", -3, {"write", "blocking"}, 1, -2, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          long double timeout_sec = 0;
          if (!parse_f64(args.back(), timeout_sec) || timeout_sec < 0) {
            return encode_error("timeout is not a float or out of range");
          }
          for (std::size_t i = 1; i + 1 < args.size(); ++i) {
            const auto tp = store().type_of(args[i]);
            if (tp != "none" && tp != "zset") return wrongtype_error_reply();
            bool wrongtype = false;
            auto vals = store().zpop(args[i], 1, false, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            if (!vals.empty()) {
              return encode_array({encode_bulk(args[i]), encode_bulk(vals[0].first), encode_bulk(format_score(vals[0].second))});
            }
          }
          // In Lua script or EXEC context: return nil immediately (no blocking)
          if (g_in_lua_script || g_executing_exec) return encode_null_array();
          session.blocked.type = BlockType::ZSet;
          session.blocked.zpop_max = false;
          session.blocked.keys.clear();
          for (std::size_t i = 1; i + 1 < args.size(); ++i) session.blocked.keys.push_back(args[i]);
          session.blocked.original_args = args;
          if (timeout_sec > 0) {
            session.blocked.deadline_ms = now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L);
          } else {
            session.blocked.deadline_ms = 0;
          }
          return std::string();
        }});

    t.emplace("BZPOPMAX", CommandSpec{"BZPOPMAX", -3, {"write", "blocking"}, 1, -2, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          long double timeout_sec = 0;
          if (!parse_f64(args.back(), timeout_sec) || timeout_sec < 0) {
            return encode_error("timeout is not a float or out of range");
          }
          for (std::size_t i = 1; i + 1 < args.size(); ++i) {
            const auto tp = store().type_of(args[i]);
            if (tp != "none" && tp != "zset") return wrongtype_error_reply();
            bool wrongtype = false;
            auto vals = store().zpop(args[i], 1, true, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            if (!vals.empty()) {
              return encode_array({encode_bulk(args[i]), encode_bulk(vals[0].first), encode_bulk(format_score(vals[0].second))});
            }
          }
          // In Lua script or EXEC context: return nil immediately (no blocking)
          if (g_in_lua_script || g_executing_exec) return encode_null_array();
          session.blocked.type = BlockType::ZSet;
          session.blocked.zpop_max = true;
          session.blocked.keys.clear();
          for (std::size_t i = 1; i + 1 < args.size(); ++i) session.blocked.keys.push_back(args[i]);
          session.blocked.original_args = args;
          if (timeout_sec > 0) {
            session.blocked.deadline_ms = now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L);
          } else {
            session.blocked.deadline_ms = 0;
          }
          return std::string();
        }});

    t.emplace("SADD", CommandSpec{"SADD", -3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_set_key(args[1], [&]() {
            std::vector<std::string> members(args.begin() + 2, args.end());
            bool wrongtype = false;
            auto n = store().sadd(args[1], members, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(n);
          });
        }});

    t.emplace("SREM", CommandSpec{"SREM", -3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_set_key(args[1], [&]() {
            std::vector<std::string> members(args.begin() + 2, args.end());
            bool wrongtype = false;
            auto n = store().srem(args[1], members, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(n);
          });
        }});

    t.emplace("SISMEMBER", CommandSpec{"SISMEMBER", 3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_set_key(args[1], [&]() {
            bool wrongtype = false;
            auto ok = store().sismember(args[1], args[2], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(ok ? 1 : 0);
          });
        }});

    t.emplace("SMEMBERS", CommandSpec{"SMEMBERS", 2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_set_key(args[1], [&]() {
            bool wrongtype = false;
            auto members = store().smembers(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            out.reserve(members.size());
            for (const auto& m : members) out.push_back(encode_bulk(m));
            return encode_array(out);
          });
        }});

    t.emplace("SCARD", CommandSpec{"SCARD", 2, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_set_key(args[1], [&]() {
            bool wrongtype = false;
            auto n = store().scard(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(n);
          });
        }});

    t.emplace("SPOP", CommandSpec{"SPOP", -2, {"write", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_set_key(args[1], [&]() {
            bool wrongtype = false;
            if (args.size() >= 3) {
              std::int64_t count = 0;
              if (!parse_i64(args[2], count) || count < 0) return encode_error("value is not an integer or out of range");
              std::vector<std::string> popped;
              for (std::int64_t i = 0; i < count; ++i) {
                auto v = store().spop(args[1], wrongtype);
                if (wrongtype) return wrongtype_error_reply();
                if (!v.has_value()) break;
                popped.push_back(*v);
              }
              std::vector<std::string> out;
              out.reserve(popped.size());
              for (const auto& m : popped) out.push_back(encode_bulk(m));
              return encode_array(out);
            }
            auto v = store().spop(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return v.has_value() ? encode_bulk(*v) : encode_null(RespVersion::Resp2);
          });
        }});

    t.emplace("SSCAN", CommandSpec{"SSCAN", -3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::uint64_t cursor = 0;
          try { cursor = static_cast<std::uint64_t>(std::stoull(args[2])); } catch (...) { return encode_error("invalid cursor"); }
          std::size_t count = 10;
          for (std::size_t i = 3; i < args.size(); ++i) {
            if (upper(args[i]) == "COUNT" && i + 1 < args.size()) {
              try { count = static_cast<std::size_t>(std::stoull(args[++i])); } catch (...) { return encode_error("value is not an integer or out of range"); }
              continue;
            }
            return encode_error("syntax error");
          }
          return with_set_key(args[1], [&]() {
            bool wrongtype = false;
            auto [next, members] = store().sscan(args[1], cursor, count, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> inner;
            inner.reserve(members.size());
            for (const auto& m : members) inner.push_back(encode_bulk(m));
            return encode_array({encode_bulk(std::to_string(next)), encode_array(inner)});
          });
        }});

    t.emplace("ZADD", CommandSpec{"ZADD", -4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          bool nx = false, xx = false, gt = false, lt = false, ch = false, incr = false;
          std::size_t pos = 2;
          for (; pos < args.size(); ++pos) {
            const std::string o = upper(args[pos]);
            if (o == "NX") { nx = true; continue; }
            if (o == "XX") { xx = true; continue; }
            if (o == "GT") { gt = true; continue; }
            if (o == "LT") { lt = true; continue; }
            if (o == "CH") { ch = true; continue; }
            if (o == "INCR") { incr = true; continue; }
            break;
          }
          if (pos >= args.size() || ((args.size() - pos) % 2) != 0) return encode_error("syntax error");
          if (incr && (args.size() - pos) != 2) return encode_error("INCR option supports a single increment-element pair");

          return with_zset_key(args[1], [&]() {
            int added = 0, changed = 0;
            std::string incr_out;
            for (std::size_t i = pos; i < args.size(); i += 2) {
              long double score = 0;
              if (!parse_f64(args[i], score)) return encode_error("value is not a valid float");
              auto r = store().zadd_one(args[1], static_cast<double>(score), args[i + 1], nx, xx, gt, lt, incr);
              if (r.wrongtype) return wrongtype_error_reply();
              if (!r.valid) return encode_error("syntax error");
              if (r.added) ++added;
              if (r.changed) ++changed;
              if (incr) {
                incr_out = format_score(r.score);
              }
            }
            if (incr) return encode_bulk(incr_out);
            return encode_integer(ch ? changed : added);
          });
        }});

    t.emplace("GEOADD", CommandSpec{"GEOADD", -5, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          bool nx = false, xx = false, ch = false;
          std::size_t pos = 2;
          for (; pos < args.size(); ++pos) {
            const std::string o = upper(args[pos]);
            if (o == "NX") { nx = true; continue; }
            if (o == "XX") { xx = true; continue; }
            if (o == "CH") { ch = true; continue; }
            break;
          }
          if (pos >= args.size() || ((args.size() - pos) % 3) != 0) return encode_error("syntax error");
          return with_zset_key(args[1], [&]() {
            int added = 0, changed = 0;
            for (std::size_t i = pos; i < args.size(); i += 3) {
              long double lon = 0;
              long double lat = 0;
              if (!parse_f64(args[i], lon) || !parse_f64(args[i + 1], lat)) return encode_error("value is not a valid float");
              // Encode lon/lat as a 52-bit interleaved geohash (matching Redis).
              auto geohash_encode = [](double longitude, double latitude) -> double {
                constexpr int BITS = 26;
                double lon_min = -180, lon_max = 180;
                double lat_min = -90, lat_max = 90;
                std::uint64_t bits = 0;
                for (int i = 0; i < BITS; ++i) {
                  double mid;
                  // Interleave: even bits = longitude, odd bits = latitude
                  mid = (lon_min + lon_max) / 2;
                  if (longitude >= mid) { bits = (bits << 1) | 1; lon_min = mid; }
                  else { bits <<= 1; lon_max = mid; }
                  mid = (lat_min + lat_max) / 2;
                  if (latitude >= mid) { bits = (bits << 1) | 1; lat_min = mid; }
                  else { bits <<= 1; lat_max = mid; }
                }
                return static_cast<double>(bits);
              };
              const double score = geohash_encode(static_cast<double>(lon), static_cast<double>(lat));
              auto r = store().zadd_one(args[1], score, args[i + 2], nx, xx, false, false, false);
              if (r.wrongtype) return wrongtype_error_reply();
              if (!r.valid) return encode_error("syntax error");
              if (r.added) ++added;
              if (r.changed) ++changed;
            }
            return encode_integer(ch ? changed : added);
          });
        }});

    t.emplace("ZRANGE", CommandSpec{"ZRANGE", -4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t start = 0, stop = 0;
          if (!parse_i64(args[2], start) || !parse_i64(args[3], stop)) return encode_error("value is not an integer or out of range");
          bool withscores = false;
          for (std::size_t i = 4; i < args.size(); ++i) {
            if (upper(args[i]) == "WITHSCORES") { withscores = true; continue; }
            return encode_error("syntax error");
          }
          return with_zset_key(args[1], [&]() {
            bool wrongtype = false;
            auto vals = store().zrange(args[1], start, stop, withscores, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            for (const auto& [m, s] : vals) {
              out.push_back(encode_bulk(m));
              if (withscores) {
                out.push_back(encode_bulk(format_score(s)));
              }
            }
            return encode_array(out);
          });
        }});

    t.emplace("ZSCAN", CommandSpec{"ZSCAN", -3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::uint64_t cursor = 0;
          try { cursor = static_cast<std::uint64_t>(std::stoull(args[2])); } catch (...) { return encode_error("invalid cursor"); }
          std::size_t count = 10;
          for (std::size_t i = 3; i < args.size(); ++i) {
            if (upper(args[i]) == "COUNT" && i + 1 < args.size()) {
              try { count = static_cast<std::size_t>(std::stoull(args[++i])); } catch (...) { return encode_error("value is not an integer or out of range"); }
              continue;
            }
            return encode_error("syntax error");
          }
          return with_zset_key(args[1], [&]() {
            bool wrongtype = false;
            auto [next, vals] = store().zscan(args[1], cursor, count, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> inner;
            for (const auto& [m, s] : vals) {
              inner.push_back(encode_bulk(m));
              inner.push_back(encode_bulk(format_score(s)));
            }
            return encode_array({encode_bulk(std::to_string(next)), encode_array(inner)});
          });
        }});

    t.emplace("ZPOPMIN", CommandSpec{"ZPOPMIN", -2, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::size_t count = 1;
          if (args.size() == 3) {
            try { count = static_cast<std::size_t>(std::stoull(args[2])); } catch (...) { return encode_error("value is not an integer or out of range"); }
          }
          return with_zset_key(args[1], [&]() {
            bool wrongtype = false;
            auto vals = store().zpop(args[1], count, false, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            for (const auto& [m, s] : vals) {
              out.push_back(encode_bulk(m));
              out.push_back(encode_bulk(format_score(s)));
            }
            return encode_array(out);
          });
        }});

    t.emplace("ZPOPMAX", CommandSpec{"ZPOPMAX", -2, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::size_t count = 1;
          if (args.size() == 3) {
            try { count = static_cast<std::size_t>(std::stoull(args[2])); } catch (...) { return encode_error("value is not an integer or out of range"); }
          }
          return with_zset_key(args[1], [&]() {
            bool wrongtype = false;
            auto vals = store().zpop(args[1], count, true, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            for (const auto& [m, s] : vals) {
              out.push_back(encode_bulk(m));
              out.push_back(encode_bulk(format_score(s)));
            }
            return encode_array(out);
          });
        }});

    t.emplace("ZMPOP", CommandSpec{"ZMPOP", -4, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t numkeys = 0;
          if (!parse_i64(args[1], numkeys) || numkeys <= 0) {
            return encode_error("numkeys should be greater than 0");
          }
          if (args.size() < static_cast<std::size_t>(3 + numkeys)) return encode_error("syntax error");
          std::size_t pos = static_cast<std::size_t>(2 + numkeys);
          const auto dir = upper(args[pos]);
          if (dir != "MIN" && dir != "MAX") return encode_error("syntax error");
          bool max = (dir == "MAX");
          std::size_t count = 1;
          if (pos + 1 < args.size()) {
            if (pos + 3 != args.size() || upper(args[pos + 1]) != "COUNT") return encode_error("syntax error");
            std::int64_t c = 0;
            if (!parse_i64(args[pos + 2], c) || c <= 0) return encode_error("count should be greater than 0");
            count = static_cast<std::size_t>(c);
          }
          for (std::int64_t i = 0; i < numkeys; ++i) {
            const auto& key = args[static_cast<std::size_t>(2 + i)];
            bool wrongtype = false;
            auto vals = store().zpop(key, count, max, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            if (vals.empty()) continue;
            std::vector<std::string> flat;
            for (const auto& [m, s] : vals) {
              flat.push_back(encode_bulk(m));
              flat.push_back(encode_bulk(format_score(s)));
            }
            return encode_array({encode_bulk(key), encode_array(flat)});
          }
          return encode_null(RespVersion::Resp2);
        }});

    t.emplace("XINFO", CommandSpec{"XINFO", -3, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (upper(args[1]) != "STREAM") return encode_error("unknown subcommand for XINFO");
          if (args.size() < 3) return encode_error("wrong number of arguments for 'xinfo|stream' command");
          if (args.size() >= 4 && upper(args[3]) != "FULL") return encode_error("syntax error");
          return with_stream_key(args[2], [&]() {
            auto digest = store().debug_digest_value(args[2]);
            if (!digest.has_value()) return encode_error("no such key");
            return encode_bulk(*digest);
          });
        }});

    t.emplace("XADD", CommandSpec{"XADD", -5, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          // Parse optional flags: [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] id field value ...
          std::size_t idx = 2;
          bool nomkstream = false;
          std::int64_t maxlen = -1;
          std::string minid_threshold;
          bool trim_approx = false;

          while (idx < args.size()) {
            const auto opt = upper(args[idx]);
            if (opt == "NOMKSTREAM") { nomkstream = true; ++idx; continue; }
            if (opt == "MAXLEN" || opt == "MINID") {
              bool is_maxlen = (opt == "MAXLEN");
              ++idx;
              if (idx >= args.size()) return encode_error("syntax error");
              // Optional = or ~
              if (args[idx] == "~" || args[idx] == "=") { trim_approx = (args[idx] == "~"); ++idx; }
              if (idx >= args.size()) return encode_error("syntax error");
              if (is_maxlen) {
                if (!parse_i64(args[idx], maxlen)) return encode_error("value is not an integer or out of range");
              } else {
                minid_threshold = args[idx];
              }
              ++idx;
              // Optional LIMIT count
              if (idx < args.size() && upper(args[idx]) == "LIMIT") {
                idx += 2; // skip LIMIT and count (we don't enforce it)
              }
              continue;
            }
            break; // First non-option is the ID
          }
          if (idx >= args.size()) return encode_error("wrong number of arguments for 'xadd' command");
          const std::string& stream_id = args[idx];
          ++idx;
          if (((args.size() - idx) % 2) != 0) return encode_error("wrong number of arguments for 'xadd' command");
          if (args.size() == idx) return encode_error("wrong number of arguments for 'xadd' command");

          // If NOMKSTREAM and key doesn't exist, return null
          if (nomkstream && store().type_of(args[1]) == "none") return encode_null(RespVersion::Resp2);

          return with_stream_key(args[1], [&]() {
            std::vector<std::pair<std::string, std::string>> fields;
            for (std::size_t i = idx; i < args.size(); i += 2) fields.push_back({args[i], args[i + 1]});
            bool wrongtype = false;
            std::string err;
            auto id = store().xadd(args[1], stream_id, fields, wrongtype, err);
            if (wrongtype) return wrongtype_error_reply();
            if (!id.has_value()) return encode_error(err.empty() ? "ERR" : err);

            // Apply MAXLEN trimming after the add
            if (maxlen >= 0) {
              bool wt = false;
              auto entries = store().xrange(args[1], "-", "+", false, wt);
              if (!wt && static_cast<std::int64_t>(entries.size()) > maxlen) {
                std::size_t to_remove = entries.size() - static_cast<std::size_t>(maxlen);
                std::vector<std::string> ids_to_del;
                for (std::size_t i = 0; i < to_remove; ++i) ids_to_del.push_back(entries[i].first);
                store().xdel(args[1], ids_to_del, wt);
              }
            }
            // Apply MINID trimming
            if (!minid_threshold.empty()) {
              bool wt = false;
              auto entries = store().xrange(args[1], "-", "+", false, wt);
              if (!wt) {
                std::vector<std::string> ids_to_del;
                for (const auto& [eid, _] : entries) {
                  if (eid < minid_threshold) ids_to_del.push_back(eid);
                }
                if (!ids_to_del.empty()) store().xdel(args[1], ids_to_del, wt);
              }
            }

            return encode_bulk(*id);
          });
        }});

    t.emplace("XDEL", CommandSpec{"XDEL", -3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::vector<std::string> ids(args.begin() + 2, args.end());
          return with_stream_key(args[1], [&]() {
            bool wrongtype = false;
            const auto n = store().xdel(args[1], ids, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(n);
          });
        }});

    t.emplace("XLEN", CommandSpec{"XLEN", 2, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_stream_key(args[1], [&]() {
            bool wrongtype = false;
            auto n = store().xlen(args[1], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(n);
          });
        }});

    t.emplace("XRANGE", CommandSpec{"XRANGE", 4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_stream_key(args[1], [&]() {
            bool wrongtype = false;
            auto items = store().xrange(args[1], args[2], args[3], false, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            for (const auto& [id, fields] : items) {
              std::vector<std::string> fv;
              for (const auto& [f, v] : fields) {
                fv.push_back(encode_bulk(f));
                fv.push_back(encode_bulk(v));
              }
              out.push_back(encode_array({encode_bulk(id), encode_array(fv)}));
            }
            return encode_array(out);
          });
        }});

    t.emplace("XREVRANGE", CommandSpec{"XREVRANGE", 4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_stream_key(args[1], [&]() {
            bool wrongtype = false;
            auto items = store().xrange(args[1], args[3], args[2], true, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> out;
            for (const auto& [id, fields] : items) {
              std::vector<std::string> fv;
              for (const auto& [f, v] : fields) {
                fv.push_back(encode_bulk(f));
                fv.push_back(encode_bulk(v));
              }
              out.push_back(encode_array({encode_bulk(id), encode_array(fv)}));
            }
            return encode_array(out);
          });
        }});

    t.emplace("XGROUP", CommandSpec{"XGROUP", -2, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          const auto sub = upper(args[1]);
          if (sub == "CREATE") {
            if (args.size() < 5) return encode_error("wrong number of arguments for 'xgroup|create' command");
            bool mkstream = false;
            if (args.size() > 5) {
              if (args.size() == 6 && upper(args[5]) == "MKSTREAM") mkstream = true;
              else return encode_error("syntax error");
            }
            return with_stream_key(args[2], [&]() {
              bool wrongtype = false;
              std::string err;
              const bool ok = store().xgroup_create(args[2], args[3], args[4], mkstream, wrongtype, err);
              if (wrongtype) return wrongtype_error_reply();
              if (!ok) return encode_error(err.empty() ? "ERR" : err);
              return encode_simple("OK");
            });
          }
          if (sub == "SETID") {
            if (args.size() != 5) return encode_error("wrong number of arguments for 'xgroup|setid' command");
            return with_stream_key(args[2], [&]() {
              bool wrongtype = false;
              std::string err;
              const bool ok = store().xgroup_setid(args[2], args[3], args[4], wrongtype, err);
              if (wrongtype) return wrongtype_error_reply();
              if (!ok) return encode_error(err.empty() ? "ERR" : err);
              return encode_simple("OK");
            });
          }
          return encode_error("unknown subcommand for XGROUP");
        }});

    t.emplace("XREADGROUP", CommandSpec{"XREADGROUP", -7, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          if (upper(args[1]) != "GROUP") return encode_error("syntax error");
          if (args.size() < 7) return encode_error("wrong number of arguments for 'xreadgroup' command");
          const std::string group = args[2];
          const std::string consumer = args[3];
          std::size_t req_count = 1;
          std::size_t i = 4;
          for (;;) {
            if (i >= args.size()) return encode_error("syntax error");
            const auto opt = upper(args[i]);
            if (opt == "STREAMS") break;
            if (opt == "NOACK") {
              ++i;
              continue;
            }
            if (opt == "BLOCK") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              std::int64_t block = 0;
              if (!parse_i64(args[i + 1], block) || block < 0) return encode_error("value is not an integer or out of range");
              if (in_lua_script_context())
                return encode_error("xreadgroup command is not allowed with BLOCK option from scripts");
              i += 2;
              continue;
            }
            if (opt == "COUNT") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              std::int64_t n = 0;
              if (!parse_i64(args[i + 1], n) || n <= 0) return encode_error("value is not an integer or out of range");
              req_count = static_cast<std::size_t>(n);
              i += 2;
              continue;
            }
            return encode_error("syntax error");
          }
          ++i;
          const std::size_t rem = args.size() - i;
          if (rem == 0 || (rem % 2) != 0) return encode_error("syntax error");
          const std::size_t n = rem / 2;
          std::vector<std::pair<std::string, std::string>> streams;
          for (std::size_t k = 0; k < n; ++k) {
            streams.push_back({args[i + k], args[i + n + k]});
          }
          bool wrongtype = false;
          std::string err;
          std::unordered_map<std::string, std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>> merged_rows;
          for (std::size_t attempt = 0; attempt < req_count; ++attempt) {
            auto rows = store().xreadgroup(group, consumer, streams, wrongtype, err);
            if (wrongtype) return wrongtype_error_reply();
            if (!err.empty()) return encode_error(err);
            if (rows.empty()) break;
            for (const auto& [key, entries] : rows) {
              auto& dst = merged_rows[key];
              dst.insert(dst.end(), entries.begin(), entries.end());
            }
          }
          if (merged_rows.empty()) return encode_null(RespVersion::Resp2);

          std::vector<std::string> out;
          for (const auto& [key, entries] : merged_rows) {
            std::vector<std::string> encoded_entries;
            for (const auto& [id, fields] : entries) {
              append_replication_event({"XCLAIM", key, group, consumer, "0", id}, session);
              std::vector<std::string> fv;
              for (const auto& [f, v] : fields) {
                fv.push_back(encode_bulk(f));
                fv.push_back(encode_bulk(v));
              }
              encoded_entries.push_back(encode_array({encode_bulk(id), encode_array(fv)}));
            }
            out.push_back(encode_array({encode_bulk(key), encode_array(encoded_entries)}));
          }
          return encode_array(out);
        }});

    t.emplace("XREAD", CommandSpec{"XREAD", -4, {"readonly", "blocking"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::size_t i = 1;
          std::int64_t count = -1;
          while (i < args.size()) {
            const auto opt = upper(args[i]);
            if (opt == "COUNT") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              std::int64_t v = 0;
              if (!parse_i64(args[i + 1], v) || v < 0) return encode_error("value is not an integer or out of range");
              count = v;
              i += 2;
              continue;
            }
            if (opt == "BLOCK") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              std::int64_t v = 0;
              if (!parse_i64(args[i + 1], v) || v < 0) return encode_error("value is not an integer or out of range");
              if (in_lua_script_context())
                return encode_error("xread command is not allowed with BLOCK option from scripts");
              // Non-blocking for now: BLOCK timeout parsed but not waited on
              i += 2;
              continue;
            }
            break;
          }
          if (i >= args.size() || upper(args[i]) != "STREAMS") return encode_error("syntax error");
          ++i;
          const auto rem = args.size() - i;
          if (rem == 0 || (rem % 2) != 0) return encode_error("Unbalanced XREAD list of streams");
          const auto n = rem / 2;
          // Validate types first
          for (std::size_t k = 0; k < n; ++k) {
            const auto tp = store().type_of(args[i + k]);
            if (tp != "none" && tp != "stream") return wrongtype_error_reply();
          }
          // Read from each stream starting after the given ID
          std::vector<std::string> results;
          bool any_data = false;
          for (std::size_t k = 0; k < n; ++k) {
            const auto& stream_key = args[i + k];
            const auto& id_arg = args[i + n + k];
            std::string start_exclusive = id_arg;
            if (start_exclusive == "$") {
              // $ means "only new entries" — for non-blocking, that means nothing
              results.push_back(encode_null(RespVersion::Resp2));
              continue;
            }
            // Increment the ID to make it exclusive (entries strictly after id_arg)
            // Use xrange from (id_arg exclusive) to + 
            // We'll fetch from the start_exclusive onwards and filter
            bool wrongtype = false;
            auto entries = store().xrange(stream_key, "-", "+", false, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            // Filter entries with id > start_exclusive
            std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> filtered;
            for (const auto& e : entries) {
              // Compare stream IDs: entries strictly greater than start_exclusive
              std::uint64_t ems = 0, eseq = 0, sms = 0, sseq = 0;
              auto parse_sid = [](const std::string& sid, std::uint64_t& ms_out, std::uint64_t& seq_out) {
                const auto pos = sid.find('-');
                if (pos == std::string::npos) { ms_out = std::stoull(sid); seq_out = 0; return; }
                ms_out = std::stoull(sid.substr(0, pos));
                seq_out = std::stoull(sid.substr(pos + 1));
              };
              try {
                parse_sid(e.first, ems, eseq);
                parse_sid(start_exclusive, sms, sseq);
              } catch (...) { continue; }
              if (ems > sms || (ems == sms && eseq > sseq)) {
                filtered.push_back(e);
                if (count > 0 && static_cast<std::int64_t>(filtered.size()) >= count) break;
              }
            }
            if (filtered.empty()) {
              results.push_back(encode_null(RespVersion::Resp2));
              continue;
            }
            any_data = true;
            // Encode: [stream_key, [[id, [field, value, ...]], ...]]
            std::vector<std::string> entry_items;
            for (const auto& fe : filtered) {
              std::vector<std::string> field_vals;
              for (const auto& [f, v] : fe.second) {
                field_vals.push_back(encode_bulk(f));
                field_vals.push_back(encode_bulk(v));
              }
              entry_items.push_back(encode_array({encode_bulk(fe.first), encode_array(field_vals)}));
            }
            results.push_back(encode_array({encode_bulk(stream_key), encode_array(entry_items)}));
          }
          if (!any_data) return encode_null(RespVersion::Resp2);
          // Filter out null entries for streams with no data
          std::vector<std::string> final_results;
          for (const auto& r : results) {
            if (r != encode_null(RespVersion::Resp2)) final_results.push_back(r);
          }
          if (final_results.empty()) return encode_null(RespVersion::Resp2);
          return encode_array(final_results);
        }});

    t.emplace("XACK", CommandSpec{"XACK", -4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::vector<std::string> ids(args.begin() + 3, args.end());
          return with_stream_key(args[1], [&]() {
            bool wrongtype = false;
            std::string err;
            auto n = store().xack(args[1], args[2], ids, wrongtype, err);
            if (wrongtype) return wrongtype_error_reply();
            if (!err.empty()) return encode_error(err);
            return encode_integer(n);
          });
        }});

    t.emplace("XPENDING", CommandSpec{"XPENDING", 3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_stream_key(args[1], [&]() {
            bool wrongtype = false;
            std::string err;
            auto sum = store().xpending_summary(args[1], args[2], wrongtype, err);
            if (wrongtype) return wrongtype_error_reply();
            if (!err.empty()) return encode_error(err);
            std::vector<std::string> out;
            out.push_back(encode_integer(sum.empty() ? 0 : std::stoll(sum[0])));
            out.push_back(sum.size() > 1 && !sum[1].empty() ? encode_bulk(sum[1]) : encode_null(RespVersion::Resp2));
            out.push_back(sum.size() > 2 && !sum[2].empty() ? encode_bulk(sum[2]) : encode_null(RespVersion::Resp2));
            out.push_back("*0\r\n");
            return encode_array(out);
          });
        }});

    // ─── SUBSCRIBE / UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE ────────────
    t.emplace("SUBSCRIBE", CommandSpec{"SUBSCRIBE", -2, {"pubsub"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::string out;
          for (std::size_t i = 1; i < args.size(); ++i) {
            pubsub_subscribe(session.fd, args[i]);
            auto& chans = g_fd_channels[session.fd];
            auto& pats = g_fd_patterns[session.fd];
            long long total = static_cast<long long>(chans.size() + pats.size());
            out += encode_array({encode_bulk("subscribe"), encode_bulk(args[i]),
                                 encode_integer(total)});
          }
          return out;
        }});

    t.emplace("UNSUBSCRIBE", CommandSpec{"UNSUBSCRIBE", -1, {"pubsub"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::string out;
          if (args.size() == 1) {
            // Unsubscribe all channels
            auto cit = g_fd_channels.find(session.fd);
            if (cit != g_fd_channels.end()) {
              auto channels_copy = cit->second; // copy since we modify during iteration
              for (const auto& ch : channels_copy) {
                pubsub_unsubscribe(session.fd, ch);
              }
            }
            auto& pats = g_fd_patterns[session.fd];
            out = encode_array({encode_bulk("unsubscribe"), encode_null(RespVersion::Resp2),
                                encode_integer(static_cast<long long>(pats.size()))});
          } else {
            for (std::size_t i = 1; i < args.size(); ++i) {
              pubsub_unsubscribe(session.fd, args[i]);
              auto& chans = g_fd_channels[session.fd];
              auto& pats = g_fd_patterns[session.fd];
              long long total = static_cast<long long>(chans.size() + pats.size());
              out += encode_array({encode_bulk("unsubscribe"), encode_bulk(args[i]),
                                   encode_integer(total)});
            }
          }
          return out;
        }});

    t.emplace("PSUBSCRIBE", CommandSpec{"PSUBSCRIBE", -2, {"pubsub"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::string out;
          for (std::size_t i = 1; i < args.size(); ++i) {
            pubsub_psubscribe(session.fd, args[i]);
            auto& chans = g_fd_channels[session.fd];
            auto& pats = g_fd_patterns[session.fd];
            long long total = static_cast<long long>(chans.size() + pats.size());
            out += encode_array({encode_bulk("psubscribe"), encode_bulk(args[i]),
                                 encode_integer(total)});
          }
          return out;
        }});

    t.emplace("PUNSUBSCRIBE", CommandSpec{"PUNSUBSCRIBE", -1, {"pubsub"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::string out;
          if (args.size() == 1) {
            auto pit = g_fd_patterns.find(session.fd);
            if (pit != g_fd_patterns.end()) {
              auto patterns_copy = pit->second;
              for (const auto& pat : patterns_copy) {
                pubsub_punsubscribe(session.fd, pat);
              }
            }
            auto& chans = g_fd_channels[session.fd];
            out = encode_array({encode_bulk("punsubscribe"), encode_null(RespVersion::Resp2),
                                encode_integer(static_cast<long long>(chans.size()))});
          } else {
            for (std::size_t i = 1; i < args.size(); ++i) {
              pubsub_punsubscribe(session.fd, args[i]);
              auto& chans = g_fd_channels[session.fd];
              auto& pats = g_fd_patterns[session.fd];
              long long total = static_cast<long long>(chans.size() + pats.size());
              out += encode_array({encode_bulk("punsubscribe"), encode_bulk(args[i]),
                                   encode_integer(total)});
            }
          }
          return out;
        }});

    // ─── CLIENT ────────────────────────────────────────────────────────
    t.emplace("CLIENT", CommandSpec{"CLIENT", -2, {"admin", "loading", "stale"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          const auto sub = upper(args[1]);
          if (sub == "SETNAME") {
            if (args.size() != 3) return encode_error("wrong number of arguments for 'client|setname' command");
            // Redis rejects names with any character <= ASCII 32.
            for (unsigned char c : args[2]) {
              if (c <= 32) return encode_error("Client names cannot contain spaces, newlines or special characters.");
            }
            session.client_name = args[2];
            return encode_simple("OK");
          }
          if (sub == "GETNAME") {
            if (session.client_name.empty()) return encode_null(session.resp_version);
            return encode_bulk(session.client_name);
          }
          if (sub == "ID") {
            return encode_integer(static_cast<long long>(session.client_id));
          }
          if (sub == "SETINFO") {
            if (args.size() != 4) return encode_error("wrong number of arguments for 'client|setinfo' command");
            const auto attr = upper(args[2]);
            if (attr == "LIB-NAME") {
              session.lib_name = args[3];
              return encode_simple("OK");
            }
            if (attr == "LIB-VER") {
              session.lib_ver = args[3];
              return encode_simple("OK");
            }
            return encode_error("Unrecognized option '" + args[2] + "'");
          }
          if (sub == "LIST") {
            if (g_client_list_callback) {
              return encode_bulk(g_client_list_callback());
            }
            return encode_bulk("");
          }
          if (sub == "INFO") {
            // Return info about the current session in CLIENT LIST format
            std::string out;
            out += "id=" + std::to_string(session.client_id);
            out += " fd=" + std::to_string(session.fd);
            out += " name=" + session.client_name;
            out += " lib-name=" + session.lib_name;
            out += " lib-ver=" + session.lib_ver;
            out += " db=" + std::to_string(session.db_index);
            out += " sub=0 psub=0 ssub=0";
            out += " multi=" + std::string(session.in_multi ? std::to_string(static_cast<int>(session.queued.size())) : "-1");
            out += " watch=" + std::to_string(session.watched_digests.size());
            out += " qbuf=0 qbuf-free=32768 argv-mem=0 multi-mem=0 tot-mem=20512";
            out += " net-i=0 net-o=0 age=0 idle=0 flags=N";
            out += " cmd=client|info user=default redir=-1";
            out += " resp=" + std::to_string(static_cast<int>(session.resp_version));
            out += "\n";
            return encode_bulk(out);
          }
          if (sub == "NO-EVICT" || sub == "NO-TOUCH") {
            return encode_simple("OK");
          }
          if (sub == "PAUSE" || sub == "UNPAUSE") {
            return encode_simple("OK");
          }
          if (sub == "REPLY") {
            return encode_simple("OK");
          }
          return encode_error("unknown subcommand or wrong number of arguments for 'client|" + lower(args[1]) + "' command");
        }});

    // ─── MEMORY ──────────────────────────────────────────────────────────
    t.emplace("MEMORY", CommandSpec{"MEMORY", -2, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          const auto sub = upper(args[1]);
          if (sub == "USAGE") {
            if (args.size() < 3) return encode_error("wrong number of arguments for 'memory|usage' command");
            const auto& key = args[2];
            if (store().type_of(key) == "none") return encode_null(RespVersion::Resp2);
            // Approximate memory usage — return a reasonable estimate
            const auto enc = store().object_encoding(key);
            std::int64_t bytes = 64; // base overhead
            const auto type = store().type_of(key);
            if (type == "string") {
              auto val = store().get(key);
              if (val) bytes += static_cast<std::int64_t>(val->size()) + 48;
            } else if (type == "list") {
              bool wt = false;
              bytes += store().llen(key, wt) * 64 + 128;
            } else if (type == "hash") {
              bool wt = false;
              bytes += store().hlen(key, wt) * 96 + 128;
            } else if (type == "set") {
              bool wt = false;
              bytes += store().scard(key, wt) * 64 + 128;
            } else if (type == "zset") {
              // Approximate: we don't have a zcard method exposed yet, use zrange
              bool wt = false;
              auto items = store().zrange(key, 0, -1, false, wt);
              bytes += static_cast<std::int64_t>(items.size()) * 96 + 128;
            } else if (type == "stream") {
              bool wt = false;
              bytes += store().xlen(key, wt) * 128 + 256;
            }
            return encode_integer(bytes);
          }
          if (sub == "DOCTOR") {
            return encode_bulk("Sam, I have no memory problems");
          }
          if (sub == "STATS") {
            // Minimal stub matching Redis output format
            return encode_bulk("peak.allocated:0\ntotal.allocated:0\nstartup.allocated:0\n"
                               "replication.backlog:0\nclients.slaves:0\nclients.normal:0\n"
                               "aof.buffer:0\nlua.caches:0\ndbnum:16\n");
          }
          if (sub == "MALLOC-STATS") {
            return encode_bulk("Memory allocator stats not available\n");
          }
          if (sub == "PURGE") {
            return encode_simple("OK");
          }
          if (sub == "HELP") {
            std::vector<std::string> items;
            items.push_back(encode_bulk("MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"));
            items.push_back(encode_bulk("DOCTOR"));
            items.push_back(encode_bulk("MALLOC-STATS"));
            items.push_back(encode_bulk("PURGE"));
            items.push_back(encode_bulk("STATS"));
            items.push_back(encode_bulk("USAGE <key> [SAMPLES <count>]"));
            return encode_array(items);
          }
          return encode_error("unknown subcommand or wrong number of arguments for 'memory|" + lower(args[1]) + "' command");
        }});

    // ─── SLOWLOG ─────────────────────────────────────────────────────────
    t.emplace("SLOWLOG", CommandSpec{"SLOWLOG", -2, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          const auto sub = upper(args[1]);
          if (sub == "GET") {
            return encode_array({}); // empty slow log
          }
          if (sub == "LEN") {
            return encode_integer(0);
          }
          if (sub == "RESET") {
            return encode_simple("OK");
          }
          if (sub == "HELP") {
            std::vector<std::string> items;
            items.push_back(encode_bulk("SLOWLOG <subcommand> [<arg> [value] ...]. Subcommands are:"));
            items.push_back(encode_bulk("GET [<count>]"));
            items.push_back(encode_bulk("LEN"));
            items.push_back(encode_bulk("RESET"));
            return encode_array(items);
          }
          return encode_error("unknown subcommand or wrong number of arguments for 'slowlog|" + lower(args[1]) + "' command");
        }});

    // ─── LATENCY ─────────────────────────────────────────────────────────
    t.emplace("LATENCY", CommandSpec{"LATENCY", -2, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          const auto sub = upper(args[1]);
          if (sub == "LATEST" || sub == "HISTORY" || sub == "GRAPH" || sub == "HISTOGRAM") {
            return encode_array({});
          }
          if (sub == "RESET") {
            return encode_integer(0);
          }
          if (sub == "DOCTOR") {
            return encode_bulk("I have no latency reports to show you at this time.");
          }
          if (sub == "HELP") {
            std::vector<std::string> items;
            items.push_back(encode_bulk("LATENCY <subcommand> [<arg> ...]. Subcommands are:"));
            items.push_back(encode_bulk("DOCTOR"));
            items.push_back(encode_bulk("GRAPH <event>"));
            items.push_back(encode_bulk("LATEST"));
            items.push_back(encode_bulk("HISTORY <event>"));
            items.push_back(encode_bulk("RESET [<event> ...]"));
            return encode_array(items);
          }
          return encode_error("unknown subcommand or wrong number of arguments for 'latency|" + lower(args[1]) + "' command");
        }});

    // ─── TOUCH ───────────────────────────────────────────────────────────
    t.emplace("TOUCH", CommandSpec{"TOUCH", -2, {"readonly", "fast"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          long long count = 0;
          for (std::size_t i = 1; i < args.size(); ++i) {
            if (store().type_of(args[i]) != "none") ++count;
          }
          return encode_integer(count);
        }});

    // ─── ROLE ────────────────────────────────────────────────────────────
    t.emplace("ROLE", CommandSpec{"ROLE", 1, {"readonly", "fast", "loading", "stale"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) -> std::string {
          std::vector<std::string> items;
          items.push_back(encode_bulk(g_replication_role));
          if (g_replication_role == "master") {
            items.push_back(encode_integer(g_master_repl_offset));
            items.push_back(encode_array({})); // replica list
          } else {
            items.push_back(encode_bulk(g_replication_master_host));
            items.push_back(encode_integer(g_replication_master_port));
            items.push_back(encode_bulk("connected"));
            items.push_back(encode_integer(g_master_repl_offset));
          }
          return encode_array(items);
        }});

    // ─── RESET ───────────────────────────────────────────────────────────
    t.emplace("RESET", CommandSpec{"RESET", 1, {"fast", "loading", "stale", "no_auth"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) -> std::string {
          session.db_index = 0;
          session.in_multi = false;
          session.multi_dirty = false;
          session.queued.clear();
          session.watched_epoch.reset();
          session.watched_digests.clear();
          session.client_name.clear();
          session.resp_version = RespVersion::Resp2;
          return "+RESET\r\n";
        }});

    // ─── LINDEX ──────────────────────────────────────────────────────────
    t.emplace("LINDEX", CommandSpec{"LINDEX", 3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_list(args[1])) return wrongtype_error_reply();
          std::int64_t index;
          if (!parse_i64(args[2], index)) return encode_error("value is not an integer or out of range");
          bool wt = false;
          auto items = store().lrange(args[1], 0, -1, wt);
          if (wt) return wrongtype_error_reply();
          if (items.empty()) return encode_null(session.resp_version);
          std::int64_t len = static_cast<std::int64_t>(items.size());
          if (index < 0) index += len;
          if (index < 0 || index >= len) return encode_null(session.resp_version);
          return encode_bulk(items[static_cast<std::size_t>(index)]);
        }});

    // ─── LSET ────────────────────────────────────────────────────────────
    t.emplace("LSET", CommandSpec{"LSET", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_list(args[1])) return wrongtype_error_reply();
          if (store().type_of(args[1]) == "none") return encode_error("no such key");
          std::int64_t index;
          if (!parse_i64(args[2], index)) return encode_error("value is not an integer or out of range");
          bool wt = false;
          auto items = store().lrange(args[1], 0, -1, wt);
          if (wt) return wrongtype_error_reply();
          std::int64_t len = static_cast<std::int64_t>(items.size());
          if (index < 0) index += len;
          if (index < 0 || index >= len) return encode_error("index out of range");
          items[static_cast<std::size_t>(index)] = args[3];
          // Rebuild list: delete and re-push
          store().del(args[1]);
          std::vector<std::string> all(items.begin(), items.end());
          store().rpush(args[1], all, wt);
          return encode_simple("OK");
        }});

    // ─── LREM ────────────────────────────────────────────────────────────
    t.emplace("LREM", CommandSpec{"LREM", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_list(args[1])) return wrongtype_error_reply();
          std::int64_t count;
          if (!parse_i64(args[2], count)) return encode_error("value is not an integer or out of range");
          bool wt = false;
          auto items = store().lrange(args[1], 0, -1, wt);
          if (wt) return wrongtype_error_reply();
          long long removed = 0;
          std::deque<std::string> result;
          if (count > 0) {
            for (const auto& item : items) {
              if (item == args[3] && removed < count) ++removed;
              else result.push_back(item);
            }
          } else if (count < 0) {
            std::int64_t limit = -count;
            // Remove from tail
            for (auto it = items.rbegin(); it != items.rend(); ++it) {
              if (*it == args[3] && removed < limit) ++removed;
              else result.push_front(*it);
            }
          } else {
            // count == 0: remove all
            for (const auto& item : items) {
              if (item == args[3]) ++removed;
              else result.push_back(item);
            }
          }
          store().del(args[1]);
          if (!result.empty()) {
            std::vector<std::string> all(result.begin(), result.end());
            store().rpush(args[1], all, wt);
          }
          return encode_integer(removed);
        }});

    // ─── LPOS ────────────────────────────────────────────────────────────
    t.emplace("LPOS", CommandSpec{"LPOS", -3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_list(args[1])) return wrongtype_error_reply();
          std::int64_t rank = 1;
          std::int64_t count_opt = -1; // -1 means not specified
          std::int64_t maxlen = 0;
          for (std::size_t i = 3; i < args.size(); i += 2) {
            if (i + 1 >= args.size()) return encode_error("syntax error");
            const auto opt = upper(args[i]);
            if (opt == "RANK") { if (!parse_i64(args[i + 1], rank)) return encode_error("value is not an integer or out of range"); if (rank == 0) return encode_error("RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative values meaning the same but counting from the end of the list."); }
            else if (opt == "COUNT") { if (!parse_i64(args[i + 1], count_opt)) return encode_error("value is not an integer or out of range"); if (count_opt < 0) return encode_error("COUNT can't be negative"); }
            else if (opt == "MAXLEN") { if (!parse_i64(args[i + 1], maxlen)) return encode_error("value is not an integer or out of range"); if (maxlen < 0) return encode_error("MAXLEN can't be negative"); }
            else return encode_error("syntax error");
          }
          bool wt = false;
          auto items = store().lrange(args[1], 0, -1, wt);
          if (wt) return wrongtype_error_reply();
          bool return_array = (count_opt >= 0);
          std::int64_t max_results = (count_opt == 0) ? static_cast<std::int64_t>(items.size()) : (count_opt > 0 ? count_opt : 1);
          std::vector<long long> positions;
          std::int64_t skip = (rank > 0) ? rank - 1 : -rank - 1;
          if (rank > 0) {
            std::int64_t limit = (maxlen > 0) ? std::min(maxlen, static_cast<std::int64_t>(items.size())) : static_cast<std::int64_t>(items.size());
            for (std::int64_t j = 0; j < limit && static_cast<std::int64_t>(positions.size()) < max_results; ++j) {
              if (items[static_cast<std::size_t>(j)] == args[2]) {
                if (skip > 0) { --skip; continue; }
                positions.push_back(j);
              }
            }
          } else {
            std::int64_t start = static_cast<std::int64_t>(items.size()) - 1;
            std::int64_t limit_pos = (maxlen > 0) ? std::max(start - maxlen + 1, static_cast<std::int64_t>(0)) : 0;
            for (std::int64_t j = start; j >= limit_pos && static_cast<std::int64_t>(positions.size()) < max_results; --j) {
              if (items[static_cast<std::size_t>(j)] == args[2]) {
                if (skip > 0) { --skip; continue; }
                positions.push_back(j);
              }
            }
          }
          if (return_array) {
            std::vector<std::string> enc;
            for (auto p : positions) enc.push_back(encode_integer(p));
            return encode_array(enc);
          }
          if (positions.empty()) return encode_null(session.resp_version);
          return encode_integer(positions[0]);
        }});

    // ─── LPUSHX ──────────────────────────────────────────────────────────
    t.emplace("LPUSHX", CommandSpec{"LPUSHX", -3, {"write", "denyoom"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_list(args[1])) return wrongtype_error_reply();
          if (store().type_of(args[1]) == "none") return encode_integer(0);
          std::vector<std::string> vals(args.begin() + 2, args.end());
          bool wt = false;
          auto len = store().lpush(args[1], vals, wt);
          if (wt) return wrongtype_error_reply();
          return encode_integer(len);
        }});

    // ─── RPUSHX ──────────────────────────────────────────────────────────
    t.emplace("RPUSHX", CommandSpec{"RPUSHX", -3, {"write", "denyoom"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_list(args[1])) return wrongtype_error_reply();
          if (store().type_of(args[1]) == "none") return encode_integer(0);
          std::vector<std::string> vals(args.begin() + 2, args.end());
          bool wt = false;
          auto len = store().rpush(args[1], vals, wt);
          if (wt) return wrongtype_error_reply();
          return encode_integer(len);
        }});

    // ─── LINSERT ─────────────────────────────────────────────────────────
    t.emplace("LINSERT", CommandSpec{"LINSERT", 5, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_list(args[1])) return wrongtype_error_reply();
          if (store().type_of(args[1]) == "none") return encode_integer(0);
          const auto dir = upper(args[2]);
          if (dir != "BEFORE" && dir != "AFTER") return encode_error("syntax error");
          bool wt = false;
          auto items = store().lrange(args[1], 0, -1, wt);
          if (wt) return wrongtype_error_reply();
          bool found = false;
          std::vector<std::string> result;
          for (const auto& item : items) {
            if (!found && item == args[3]) {
              found = true;
              if (dir == "BEFORE") { result.push_back(args[4]); result.push_back(item); }
              else { result.push_back(item); result.push_back(args[4]); }
            } else {
              result.push_back(item);
            }
          }
          if (!found) return encode_integer(-1);
          store().del(args[1]);
          store().rpush(args[1], result, wt);
          return encode_integer(static_cast<long long>(result.size()));
        }});

    // ─── RPOPLPUSH ───────────────────────────────────────────────────────
    t.emplace("RPOPLPUSH", CommandSpec{"RPOPLPUSH", 3, {"write"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_list(args[1])) return wrongtype_error_reply();
          if (store().is_wrongtype_for_list(args[2])) return wrongtype_error_reply();
          bool wt = false;
          auto val = store().rpop(args[1], wt);
          if (wt) return wrongtype_error_reply();
          if (!val) return encode_null(session.resp_version);
          store().lpush(args[2], {*val}, wt);
          return encode_bulk(*val);
        }});

    // ─── LTRIM ───────────────────────────────────────────────────────────
    t.emplace("LTRIM", CommandSpec{"LTRIM", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_list(args[1])) return wrongtype_error_reply();
          std::int64_t start, stop;
          if (!parse_i64(args[2], start) || !parse_i64(args[3], stop))
            return encode_error("value is not an integer or out of range");
          bool wt = false;
          auto items = store().lrange(args[1], start, stop, wt);
          if (wt) return wrongtype_error_reply();
          store().del(args[1]);
          if (!items.empty()) store().rpush(args[1], items, wt);
          return encode_simple("OK");
        }});

    // ─── LMPOP ───────────────────────────────────────────────────────────
    t.emplace("LMPOP", CommandSpec{"LMPOP", -4, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          std::int64_t numkeys;
          if (!parse_i64(args[1], numkeys) || numkeys <= 0)
            return encode_error("numkeys can't be non-positive value");
          if (static_cast<std::size_t>(numkeys + 2) >= args.size())
            return encode_error("syntax error");
          std::size_t dir_idx = static_cast<std::size_t>(numkeys) + 2;
          if (dir_idx >= args.size()) return encode_error("syntax error");
          const auto dir = upper(args[dir_idx]);
          if (dir != "LEFT" && dir != "RIGHT") return encode_error("syntax error");
          std::int64_t count = 1;
          for (std::size_t i = dir_idx + 1; i < args.size(); i += 2) {
            if (i + 1 >= args.size()) return encode_error("syntax error");
            if (upper(args[i]) == "COUNT") {
              if (!parse_i64(args[i + 1], count) || count <= 0) return encode_error("value is not an integer or out of range");
            } else return encode_error("syntax error");
          }
          for (std::int64_t k = 0; k < numkeys; ++k) {
            const auto& key = args[static_cast<std::size_t>(k + 2)];
            if (store().is_wrongtype_for_list(key)) return wrongtype_error_reply();
            bool wt = false;
            if (store().llen(key, wt) == 0) continue;
            std::vector<std::string> popped;
            for (std::int64_t c = 0; c < count; ++c) {
              auto val = (dir == "LEFT") ? store().lpop(key, wt) : store().rpop(key, wt);
              if (!val) break;
              popped.push_back(*val);
            }
            if (popped.empty()) continue;
            std::vector<std::string> enc;
            for (const auto& v : popped) enc.push_back(encode_bulk(v));
            return encode_array({encode_bulk(key), encode_array(enc)});
          }
          return encode_null_array();
        }});

    // ─── ZCARD ───────────────────────────────────────────────────────────
    t.emplace("ZCARD", CommandSpec{"ZCARD", 2, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, false, wt);
          if (wt) return wrongtype_error_reply();
          return encode_integer(static_cast<long long>(items.size()));
        }});

    // ─── ZSCORE ──────────────────────────────────────────────────────────
    t.emplace("ZSCORE", CommandSpec{"ZSCORE", 3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          for (const auto& [member, score] : items) {
            if (member == args[2]) {
              return encode_bulk(format_score(score));
            }
          }
          return encode_null(session.resp_version);
        }});

    // ─── ZMSCORE ─────────────────────────────────────────────────────────
    t.emplace("ZMSCORE", CommandSpec{"ZMSCORE", -3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          std::unordered_map<std::string, double> score_map;
          for (const auto& [m, s] : items) score_map[m] = s;
          std::vector<std::string> results;
          for (std::size_t i = 2; i < args.size(); ++i) {
            auto it = score_map.find(args[i]);
            if (it != score_map.end()) {
              results.push_back(encode_bulk(format_score(it->second)));
            } else {
              results.push_back(encode_null(session.resp_version));
            }
          }
          return encode_array(results);
        }});

    // ─── ZCOUNT ──────────────────────────────────────────────────────────
    t.emplace("ZCOUNT", CommandSpec{"ZCOUNT", 4, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          auto parse_bound = [](const std::string& s, double& val, bool& exclusive) -> bool {
            exclusive = false;
            if (s == "-inf") { val = -std::numeric_limits<double>::infinity(); return true; }
            if (s == "+inf" || s == "inf") { val = std::numeric_limits<double>::infinity(); return true; }
            std::string v = s;
            if (!v.empty() && v[0] == '(') { exclusive = true; v = v.substr(1); }
            try { val = std::stod(v); return true; } catch (...) { return false; }
          };
          double min_val, max_val;
          bool min_ex, max_ex;
          if (!parse_bound(args[2], min_val, min_ex) || !parse_bound(args[3], max_val, max_ex))
            return encode_error("min or max is not a float");
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          long long count = 0;
          for (const auto& [m, s] : items) {
            bool above_min = min_ex ? (s > min_val) : (s >= min_val);
            bool below_max = max_ex ? (s < max_val) : (s <= max_val);
            if (above_min && below_max) ++count;
          }
          return encode_integer(count);
        }});

    // ─── ZRANK ───────────────────────────────────────────────────────────
    t.emplace("ZRANK", CommandSpec{"ZRANK", -3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          // Sort by score ascending (zrange should already be sorted)
          for (std::size_t i = 0; i < items.size(); ++i) {
            if (items[i].first == args[2]) {
              bool withscore = (args.size() >= 4 && upper(args[3]) == "WITHSCORE");
              if (withscore) {
                return encode_array({encode_integer(static_cast<long long>(i)), encode_bulk(format_score(items[i].second))});
              }
              return encode_integer(static_cast<long long>(i));
            }
          }
          return encode_null(session.resp_version);
        }});

    // ─── ZREVRANK ────────────────────────────────────────────────────────
    t.emplace("ZREVRANK", CommandSpec{"ZREVRANK", -3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          for (std::size_t i = 0; i < items.size(); ++i) {
            if (items[i].first == args[2]) {
              bool withscore = (args.size() >= 4 && upper(args[3]) == "WITHSCORE");
              long long rev_rank = static_cast<long long>(items.size() - 1 - i);
              if (withscore) {
                return encode_array({encode_integer(rev_rank), encode_bulk(format_score(items[i].second))});
              }
              return encode_integer(rev_rank);
            }
          }
          return encode_null(session.resp_version);
        }});

    // ─── ZREM ────────────────────────────────────────────────────────────
    t.emplace("ZREM", CommandSpec{"ZREM", -3, {"write", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          std::unordered_set<std::string> to_remove(args.begin() + 2, args.end());
          long long removed = 0;
          // Delete and re-add remaining
          std::vector<std::pair<std::string, double>> remaining;
          for (const auto& [m, s] : items) {
            if (to_remove.count(m)) ++removed;
            else remaining.push_back({m, s});
          }
          if (removed > 0) {
            store().del(args[1]);
            for (const auto& [m, s] : remaining) {
              store().zadd_one(args[1], s, m, false, false, false, false, false);
            }
          }
          return encode_integer(removed);
        }});

    // ─── ZINCRBY ─────────────────────────────────────────────────────────
    t.emplace("ZINCRBY", CommandSpec{"ZINCRBY", 4, {"write", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          double incr;
          try { incr = std::stod(args[2]); } catch (...) { return encode_error("value is not a valid float"); }
          auto result = store().zadd_one(args[1], incr, args[3], false, false, false, false, true);
          if (result.wrongtype) return wrongtype_error_reply();
          return encode_bulk(format_score(result.score));
        }});

    // ─── ZRANGEBYSCORE ───────────────────────────────────────────────────
    t.emplace("ZRANGEBYSCORE", CommandSpec{"ZRANGEBYSCORE", -4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          auto parse_bound = [](const std::string& s, double& val, bool& exclusive) -> bool {
            exclusive = false;
            if (s == "-inf") { val = -std::numeric_limits<double>::infinity(); return true; }
            if (s == "+inf" || s == "inf") { val = std::numeric_limits<double>::infinity(); return true; }
            std::string v = s;
            if (!v.empty() && v[0] == '(') { exclusive = true; v = v.substr(1); }
            try { val = std::stod(v); return true; } catch (...) { return false; }
          };
          double min_val, max_val;
          bool min_ex, max_ex;
          if (!parse_bound(args[2], min_val, min_ex) || !parse_bound(args[3], max_val, max_ex))
            return encode_error("min or max is not a float");
          bool withscores = false;
          std::int64_t offset = 0, limit_count = -1;
          for (std::size_t i = 4; i < args.size(); ++i) {
            if (upper(args[i]) == "WITHSCORES") withscores = true;
            else if (upper(args[i]) == "LIMIT" && i + 2 < args.size()) {
              if (!parse_i64(args[i + 1], offset) || !parse_i64(args[i + 2], limit_count))
                return encode_error("value is not an integer or out of range");
              i += 2;
            }
          }
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          std::vector<std::pair<std::string, double>> matched;
          for (const auto& [m, s] : items) {
            bool above = min_ex ? (s > min_val) : (s >= min_val);
            bool below = max_ex ? (s < max_val) : (s <= max_val);
            if (above && below) matched.push_back({m, s});
          }
          // Apply LIMIT
          if (offset > 0) {
            if (static_cast<std::size_t>(offset) >= matched.size()) matched.clear();
            else matched.erase(matched.begin(), matched.begin() + offset);
          }
          if (limit_count >= 0 && static_cast<std::size_t>(limit_count) < matched.size()) {
            matched.resize(static_cast<std::size_t>(limit_count));
          }
          std::vector<std::string> enc;
          for (const auto& [m, s] : matched) {
            enc.push_back(encode_bulk(m));
            if (withscores) { enc.push_back(encode_bulk(format_score(s))); }
          }
          return encode_array(enc);
        }});

    // ─── ZREVRANGEBYSCORE ────────────────────────────────────────────────
    t.emplace("ZREVRANGEBYSCORE", CommandSpec{"ZREVRANGEBYSCORE", -4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          auto parse_bound = [](const std::string& s, double& val, bool& exclusive) -> bool {
            exclusive = false;
            if (s == "-inf") { val = -std::numeric_limits<double>::infinity(); return true; }
            if (s == "+inf" || s == "inf") { val = std::numeric_limits<double>::infinity(); return true; }
            std::string v = s;
            if (!v.empty() && v[0] == '(') { exclusive = true; v = v.substr(1); }
            try { val = std::stod(v); return true; } catch (...) { return false; }
          };
          // Note: for ZREVRANGEBYSCORE, args[2] is max, args[3] is min (reversed)
          double max_val, min_val;
          bool max_ex, min_ex;
          if (!parse_bound(args[2], max_val, max_ex) || !parse_bound(args[3], min_val, min_ex))
            return encode_error("min or max is not a float");
          bool withscores = false;
          std::int64_t offset = 0, limit_count = -1;
          for (std::size_t i = 4; i < args.size(); ++i) {
            if (upper(args[i]) == "WITHSCORES") withscores = true;
            else if (upper(args[i]) == "LIMIT" && i + 2 < args.size()) {
              if (!parse_i64(args[i + 1], offset) || !parse_i64(args[i + 2], limit_count))
                return encode_error("value is not an integer or out of range");
              i += 2;
            }
          }
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          std::vector<std::pair<std::string, double>> matched;
          for (auto it = items.rbegin(); it != items.rend(); ++it) {
            bool above = min_ex ? (it->second > min_val) : (it->second >= min_val);
            bool below = max_ex ? (it->second < max_val) : (it->second <= max_val);
            if (above && below) matched.push_back(*it);
          }
          if (offset > 0) {
            if (static_cast<std::size_t>(offset) >= matched.size()) matched.clear();
            else matched.erase(matched.begin(), matched.begin() + offset);
          }
          if (limit_count >= 0 && static_cast<std::size_t>(limit_count) < matched.size()) {
            matched.resize(static_cast<std::size_t>(limit_count));
          }
          std::vector<std::string> enc;
          for (const auto& [m, s] : matched) {
            enc.push_back(encode_bulk(m));
            if (withscores) { enc.push_back(encode_bulk(format_score(s))); }
          }
          return encode_array(enc);
        }});

    // ─── ZREVRANGE ───────────────────────────────────────────────────────
    t.emplace("ZREVRANGE", CommandSpec{"ZREVRANGE", -4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          std::int64_t start, stop;
          if (!parse_i64(args[2], start) || !parse_i64(args[3], stop))
            return encode_error("value is not an integer or out of range");
          bool withscores = (args.size() >= 5 && upper(args[4]) == "WITHSCORES");
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          // Reverse the items
          std::reverse(items.begin(), items.end());
          std::int64_t len = static_cast<std::int64_t>(items.size());
          if (start < 0) start += len;
          if (stop < 0) stop += len;
          if (start < 0) start = 0;
          if (stop >= len) stop = len - 1;
          std::vector<std::string> enc;
          if (start <= stop) {
            for (std::int64_t i = start; i <= stop; ++i) {
              enc.push_back(encode_bulk(items[static_cast<std::size_t>(i)].first));
              if (withscores) { enc.push_back(encode_bulk(format_score(items[static_cast<std::size_t>(i)].second))); }
            }
          }
          return encode_array(enc);
        }});

    // ─── ZLEXCOUNT ───────────────────────────────────────────────────────
    t.emplace("ZLEXCOUNT", CommandSpec{"ZLEXCOUNT", 4, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          auto parse_lex = [](const std::string& s, std::string& val, bool& exclusive, bool& is_unbounded) -> bool {
            is_unbounded = false;
            exclusive = false;
            if (s == "-") { is_unbounded = true; val = ""; return true; }
            if (s == "+") { is_unbounded = true; val = "\xff"; return true; }
            if (s.size() < 2) return false;
            if (s[0] == '[') { exclusive = false; val = s.substr(1); return true; }
            if (s[0] == '(') { exclusive = true; val = s.substr(1); return true; }
            return false;
          };
          std::string min_val, max_val;
          bool min_ex, max_ex, min_unb, max_unb;
          if (!parse_lex(args[2], min_val, min_ex, min_unb) || !parse_lex(args[3], max_val, max_ex, max_unb))
            return encode_error("min or max not valid string range item");
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, false, wt);
          if (wt) return wrongtype_error_reply();
          // Sort lexicographically (items from zrange are sorted by score then by member)
          std::vector<std::string> members;
          for (const auto& [m, _] : items) members.push_back(m);
          long long count = 0;
          for (const auto& m : members) {
            bool above = min_unb ? true : (min_ex ? (m > min_val) : (m >= min_val));
            bool below = max_unb ? true : (max_ex ? (m < max_val) : (m <= max_val));
            if (above && below) ++count;
          }
          return encode_integer(count);
        }});

    // ─── ZRANDMEMBER ─────────────────────────────────────────────────────
    t.emplace("ZRANDMEMBER", CommandSpec{"ZRANDMEMBER", -2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          if (items.empty()) {
            if (args.size() >= 3) return encode_array({});
            return encode_null(session.resp_version);
          }
          if (args.size() == 2) {
            auto idx = static_cast<std::size_t>(std::rand() % static_cast<int>(items.size()));
            return encode_bulk(items[idx].first);
          }
          std::int64_t count;
          if (!parse_i64(args[2], count)) return encode_error("value is not an integer or out of range");
          bool withscores = (args.size() >= 4 && upper(args[3]) == "WITHSCORES");
          bool allow_dups = (count < 0);
          std::int64_t abs_count = std::abs(count);
          std::vector<std::string> enc;
          for (std::int64_t i = 0; i < abs_count; ++i) {
            auto idx = static_cast<std::size_t>(std::rand() % static_cast<int>(items.size()));
            enc.push_back(encode_bulk(items[idx].first));
            if (withscores) { enc.push_back(encode_bulk(format_score(items[idx].second))); }
          }
          return encode_array(enc);
        }});

    // ─── ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE ─────────────────────────
    t.emplace("ZUNIONSTORE", CommandSpec{"ZUNIONSTORE", -4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::int64_t numkeys;
          if (!parse_i64(args[2], numkeys) || numkeys <= 0) return encode_error("value is not an integer or out of range");
          if (static_cast<std::size_t>(3 + numkeys) > args.size()) return encode_error("syntax error");
          // Collect all scores from all keys
          std::unordered_map<std::string, double> result;
          for (std::int64_t i = 0; i < numkeys; ++i) {
            const auto& key = args[static_cast<std::size_t>(3 + i)];
            if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
            bool wt = false;
            auto items = store().zrange(key, 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& [m, s] : items) {
              auto it = result.find(m);
              if (it != result.end()) it->second += s; // default AGGREGATE SUM
              else result[m] = s;
            }
          }
          store().del(args[1]);
          for (const auto& [m, s] : result) {
            store().zadd_one(args[1], s, m, false, false, false, false, false);
          }
          return encode_integer(static_cast<long long>(result.size()));
        }});

    t.emplace("ZINTERSTORE", CommandSpec{"ZINTERSTORE", -4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::int64_t numkeys;
          if (!parse_i64(args[2], numkeys) || numkeys <= 0) return encode_error("value is not an integer or out of range");
          if (static_cast<std::size_t>(3 + numkeys) > args.size()) return encode_error("syntax error");
          // Start with first key's members
          std::unordered_map<std::string, double> result;
          {
            const auto& key = args[3];
            if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
            bool wt = false;
            auto items = store().zrange(key, 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& [m, s] : items) result[m] = s;
          }
          for (std::int64_t i = 1; i < numkeys; ++i) {
            const auto& key = args[static_cast<std::size_t>(3 + i)];
            if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
            bool wt = false;
            auto items = store().zrange(key, 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            std::unordered_set<std::string> members_in_key;
            for (const auto& [m, s] : items) {
              members_in_key.insert(m);
              if (result.count(m)) result[m] += s;
            }
            // Remove members not in this key
            for (auto it = result.begin(); it != result.end();) {
              if (!members_in_key.count(it->first)) it = result.erase(it);
              else ++it;
            }
          }
          store().del(args[1]);
          for (const auto& [m, s] : result) {
            store().zadd_one(args[1], s, m, false, false, false, false, false);
          }
          return encode_integer(static_cast<long long>(result.size()));
        }});

    t.emplace("ZDIFFSTORE", CommandSpec{"ZDIFFSTORE", -4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::int64_t numkeys;
          if (!parse_i64(args[2], numkeys) || numkeys <= 0) return encode_error("value is not an integer or out of range");
          if (static_cast<std::size_t>(3 + numkeys) > args.size()) return encode_error("syntax error");
          std::unordered_map<std::string, double> result;
          {
            const auto& key = args[3];
            if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
            bool wt = false;
            auto items = store().zrange(key, 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& [m, s] : items) result[m] = s;
          }
          for (std::int64_t i = 1; i < numkeys; ++i) {
            const auto& key = args[static_cast<std::size_t>(3 + i)];
            if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
            bool wt = false;
            auto items = store().zrange(key, 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& [m, _] : items) result.erase(m);
          }
          store().del(args[1]);
          for (const auto& [m, s] : result) {
            store().zadd_one(args[1], s, m, false, false, false, false, false);
          }
          return encode_integer(static_cast<long long>(result.size()));
        }});

    // ─── ZUNION / ZINTER / ZDIFF (read-only) ────────────────────────────
    t.emplace("ZUNION", CommandSpec{"ZUNION", -3, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::int64_t numkeys;
          if (!parse_i64(args[1], numkeys) || numkeys <= 0) return encode_error("value is not an integer or out of range");
          if (static_cast<std::size_t>(2 + numkeys) > args.size()) return encode_error("syntax error");
          bool withscores = false;
          for (std::size_t i = static_cast<std::size_t>(2 + numkeys); i < args.size(); ++i) {
            if (upper(args[i]) == "WITHSCORES") withscores = true;
          }
          std::unordered_map<std::string, double> result;
          for (std::int64_t i = 0; i < numkeys; ++i) {
            const auto& key = args[static_cast<std::size_t>(2 + i)];
            if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
            bool wt = false;
            auto items = store().zrange(key, 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& [m, s] : items) {
              auto it = result.find(m);
              if (it != result.end()) it->second += s;
              else result[m] = s;
            }
          }
          // Sort by score, then by member
          std::vector<std::pair<std::string, double>> sorted(result.begin(), result.end());
          std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
            return a.second < b.second || (a.second == b.second && a.first < b.first);
          });
          std::vector<std::string> enc;
          for (const auto& [m, s] : sorted) {
            enc.push_back(encode_bulk(m));
            if (withscores) { enc.push_back(encode_bulk(format_score(s))); }
          }
          return encode_array(enc);
        }});

    t.emplace("ZINTER", CommandSpec{"ZINTER", -3, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::int64_t numkeys;
          if (!parse_i64(args[1], numkeys) || numkeys <= 0) return encode_error("value is not an integer or out of range");
          if (static_cast<std::size_t>(2 + numkeys) > args.size()) return encode_error("syntax error");
          bool withscores = false;
          for (std::size_t i = static_cast<std::size_t>(2 + numkeys); i < args.size(); ++i) {
            if (upper(args[i]) == "WITHSCORES") withscores = true;
          }
          std::unordered_map<std::string, double> result;
          {
            const auto& key = args[2];
            if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
            bool wt = false;
            auto items = store().zrange(key, 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& [m, s] : items) result[m] = s;
          }
          for (std::int64_t i = 1; i < numkeys; ++i) {
            const auto& key = args[static_cast<std::size_t>(2 + i)];
            if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
            bool wt = false;
            auto items = store().zrange(key, 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            std::unordered_set<std::string> members_in_key;
            for (const auto& [m, s] : items) {
              members_in_key.insert(m);
              if (result.count(m)) result[m] += s;
            }
            for (auto it = result.begin(); it != result.end();) {
              if (!members_in_key.count(it->first)) it = result.erase(it);
              else ++it;
            }
          }
          std::vector<std::pair<std::string, double>> sorted(result.begin(), result.end());
          std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
            return a.second < b.second || (a.second == b.second && a.first < b.first);
          });
          std::vector<std::string> enc;
          for (const auto& [m, s] : sorted) {
            enc.push_back(encode_bulk(m));
            if (withscores) { enc.push_back(encode_bulk(format_score(s))); }
          }
          return encode_array(enc);
        }});

    t.emplace("ZDIFF", CommandSpec{"ZDIFF", -3, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::int64_t numkeys;
          if (!parse_i64(args[1], numkeys) || numkeys <= 0) return encode_error("value is not an integer or out of range");
          if (static_cast<std::size_t>(2 + numkeys) > args.size()) return encode_error("syntax error");
          bool withscores = false;
          for (std::size_t i = static_cast<std::size_t>(2 + numkeys); i < args.size(); ++i) {
            if (upper(args[i]) == "WITHSCORES") withscores = true;
          }
          std::unordered_map<std::string, double> result;
          {
            const auto& key = args[2];
            if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
            bool wt = false;
            auto items = store().zrange(key, 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& [m, s] : items) result[m] = s;
          }
          for (std::int64_t i = 1; i < numkeys; ++i) {
            const auto& key = args[static_cast<std::size_t>(2 + i)];
            if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
            bool wt = false;
            auto items = store().zrange(key, 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& [m, _] : items) result.erase(m);
          }
          std::vector<std::pair<std::string, double>> sorted(result.begin(), result.end());
          std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
            return a.second < b.second || (a.second == b.second && a.first < b.first);
          });
          std::vector<std::string> enc;
          for (const auto& [m, s] : sorted) {
            enc.push_back(encode_bulk(m));
            if (withscores) { enc.push_back(encode_bulk(format_score(s))); }
          }
          return encode_array(enc);
        }});

    // ─── SRANDMEMBER ─────────────────────────────────────────────────────
    t.emplace("SRANDMEMBER", CommandSpec{"SRANDMEMBER", -2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_set(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto members = store().smembers(args[1], wt);
          if (wt) return wrongtype_error_reply();
          if (members.empty()) {
            if (args.size() >= 3) return encode_array({});
            return encode_null(session.resp_version);
          }
          if (args.size() == 2) {
            auto idx = static_cast<std::size_t>(std::rand() % static_cast<int>(members.size()));
            return encode_bulk(members[idx]);
          }
          std::int64_t count;
          if (!parse_i64(args[2], count)) return encode_error("value is not an integer or out of range");
          bool allow_dups = (count < 0);
          std::int64_t abs_count = std::abs(count);
          std::vector<std::string> enc;
          for (std::int64_t i = 0; i < abs_count; ++i) {
            auto idx = static_cast<std::size_t>(std::rand() % static_cast<int>(members.size()));
            enc.push_back(encode_bulk(members[idx]));
          }
          return encode_array(enc);
        }});

    // ─── SINTER ──────────────────────────────────────────────────────────
    t.emplace("SINTER", CommandSpec{"SINTER", -2, {"readonly"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          bool wt = false;
          auto result_set = store().smembers(args[1], wt);
          if (wt) return wrongtype_error_reply();
          std::unordered_set<std::string> current(result_set.begin(), result_set.end());
          for (std::size_t i = 2; i < args.size(); ++i) {
            if (store().is_wrongtype_for_set(args[i])) return wrongtype_error_reply();
            auto members = store().smembers(args[i], wt);
            if (wt) return wrongtype_error_reply();
            std::unordered_set<std::string> mset(members.begin(), members.end());
            for (auto it = current.begin(); it != current.end();) {
              if (!mset.count(*it)) it = current.erase(it);
              else ++it;
            }
          }
          std::vector<std::string> enc;
          for (const auto& m : current) enc.push_back(encode_bulk(m));
          return encode_array(enc);
        }});

    // ─── SINTERCARD ──────────────────────────────────────────────────────
    t.emplace("SINTERCARD", CommandSpec{"SINTERCARD", -3, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::int64_t numkeys;
          if (!parse_i64(args[1], numkeys) || numkeys <= 0) return encode_error("numkeys can't be non-positive value");
          if (static_cast<std::size_t>(2 + numkeys) > args.size()) return encode_error("syntax error");
          std::int64_t limit = 0;
          std::size_t opt_start = static_cast<std::size_t>(2 + numkeys);
          for (std::size_t i = opt_start; i < args.size(); i += 2) {
            if (upper(args[i]) == "LIMIT" && i + 1 < args.size()) {
              if (!parse_i64(args[i + 1], limit) || limit < 0) return encode_error("value is not an integer or out of range");
            } else return encode_error("syntax error");
          }
          bool wt = false;
          auto first_members = store().smembers(args[2], wt);
          if (wt) return wrongtype_error_reply();
          std::unordered_set<std::string> current(first_members.begin(), first_members.end());
          for (std::int64_t i = 1; i < numkeys; ++i) {
            const auto& key = args[static_cast<std::size_t>(2 + i)];
            if (store().is_wrongtype_for_set(key)) return wrongtype_error_reply();
            auto members = store().smembers(key, wt);
            if (wt) return wrongtype_error_reply();
            std::unordered_set<std::string> mset(members.begin(), members.end());
            for (auto it = current.begin(); it != current.end();) {
              if (!mset.count(*it)) it = current.erase(it);
              else ++it;
            }
          }
          long long result = static_cast<long long>(current.size());
          if (limit > 0 && result > limit) result = limit;
          return encode_integer(result);
        }});

    // ─── SUNION ──────────────────────────────────────────────────────────
    t.emplace("SUNION", CommandSpec{"SUNION", -2, {"readonly"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::unordered_set<std::string> result;
          bool wt = false;
          for (std::size_t i = 1; i < args.size(); ++i) {
            if (store().is_wrongtype_for_set(args[i])) return wrongtype_error_reply();
            auto members = store().smembers(args[i], wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& m : members) result.insert(m);
          }
          std::vector<std::string> enc;
          for (const auto& m : result) enc.push_back(encode_bulk(m));
          return encode_array(enc);
        }});

    // ─── SDIFF ───────────────────────────────────────────────────────────
    t.emplace("SDIFF", CommandSpec{"SDIFF", -2, {"readonly"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          bool wt = false;
          auto first_members = store().smembers(args[1], wt);
          if (wt) return wrongtype_error_reply();
          std::unordered_set<std::string> result(first_members.begin(), first_members.end());
          for (std::size_t i = 2; i < args.size(); ++i) {
            if (store().is_wrongtype_for_set(args[i])) return wrongtype_error_reply();
            auto members = store().smembers(args[i], wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& m : members) result.erase(m);
          }
          std::vector<std::string> enc;
          for (const auto& m : result) enc.push_back(encode_bulk(m));
          return encode_array(enc);
        }});

    // ─── SINTERSTORE / SUNIONSTORE / SDIFFSTORE ─────────────────────────
    t.emplace("SINTERSTORE", CommandSpec{"SINTERSTORE", -3, {"write"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          bool wt = false;
          auto first = store().smembers(args[2], wt);
          if (wt) return wrongtype_error_reply();
          std::unordered_set<std::string> result(first.begin(), first.end());
          for (std::size_t i = 3; i < args.size(); ++i) {
            if (store().is_wrongtype_for_set(args[i])) return wrongtype_error_reply();
            auto members = store().smembers(args[i], wt);
            if (wt) return wrongtype_error_reply();
            std::unordered_set<std::string> mset(members.begin(), members.end());
            for (auto it = result.begin(); it != result.end();) {
              if (!mset.count(*it)) it = result.erase(it);
              else ++it;
            }
          }
          store().del(args[1]);
          if (!result.empty()) {
            std::vector<std::string> vals(result.begin(), result.end());
            store().sadd(args[1], vals, wt);
          }
          return encode_integer(static_cast<long long>(result.size()));
        }});

    t.emplace("SUNIONSTORE", CommandSpec{"SUNIONSTORE", -3, {"write"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::unordered_set<std::string> result;
          bool wt = false;
          for (std::size_t i = 2; i < args.size(); ++i) {
            if (store().is_wrongtype_for_set(args[i])) return wrongtype_error_reply();
            auto members = store().smembers(args[i], wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& m : members) result.insert(m);
          }
          store().del(args[1]);
          if (!result.empty()) {
            std::vector<std::string> vals(result.begin(), result.end());
            store().sadd(args[1], vals, wt);
          }
          return encode_integer(static_cast<long long>(result.size()));
        }});

    t.emplace("SDIFFSTORE", CommandSpec{"SDIFFSTORE", -3, {"write"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          bool wt = false;
          auto first = store().smembers(args[2], wt);
          if (wt) return wrongtype_error_reply();
          std::unordered_set<std::string> result(first.begin(), first.end());
          for (std::size_t i = 3; i < args.size(); ++i) {
            if (store().is_wrongtype_for_set(args[i])) return wrongtype_error_reply();
            auto members = store().smembers(args[i], wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& m : members) result.erase(m);
          }
          store().del(args[1]);
          if (!result.empty()) {
            std::vector<std::string> vals(result.begin(), result.end());
            store().sadd(args[1], vals, wt);
          }
          return encode_integer(static_cast<long long>(result.size()));
        }});

    // ─── SMISMEMBER ──────────────────────────────────────────────────────
    t.emplace("SMISMEMBER", CommandSpec{"SMISMEMBER", -3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_set(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto members = store().smembers(args[1], wt);
          if (wt) return wrongtype_error_reply();
          std::unordered_set<std::string> mset(members.begin(), members.end());
          std::vector<std::string> enc;
          for (std::size_t i = 2; i < args.size(); ++i) {
            enc.push_back(encode_integer(mset.count(args[i]) ? 1 : 0));
          }
          return encode_array(enc);
        }});

    // ─── HRANDFIELD ──────────────────────────────────────────────────────
    t.emplace("HRANDFIELD", CommandSpec{"HRANDFIELD", -2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_hash(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto all = store().hgetall(args[1], wt);
          if (wt) return wrongtype_error_reply();
          if (all.empty()) {
            if (args.size() >= 3) return encode_array({});
            return encode_null(session.resp_version);
          }
          if (args.size() == 2) {
            auto idx = static_cast<std::size_t>(std::rand() % static_cast<int>(all.size()));
            return encode_bulk(all[idx].first);
          }
          std::int64_t count;
          if (!parse_i64(args[2], count)) return encode_error("value is not an integer or out of range");
          bool withvalues = (args.size() >= 4 && upper(args[3]) == "WITHVALUES");
          std::int64_t abs_count = std::abs(count);
          std::vector<std::string> enc;
          for (std::int64_t i = 0; i < abs_count; ++i) {
            auto idx = static_cast<std::size_t>(std::rand() % static_cast<int>(all.size()));
            enc.push_back(encode_bulk(all[idx].first));
            if (withvalues) enc.push_back(encode_bulk(all[idx].second));
          }
          return encode_array(enc);
        }});

    // ─── BITCOUNT ────────────────────────────────────────────────────────
    t.emplace("BITCOUNT", CommandSpec{"BITCOUNT", -2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_string(args[1])) return wrongtype_error_reply();
          auto val = store().get(args[1]);
          if (!val) return encode_integer(0);
          const std::string& s = *val;
          std::int64_t start = 0, end = static_cast<std::int64_t>(s.size()) - 1;
          bool bit_mode = false;
          if (args.size() >= 4) {
            if (!parse_i64(args[2], start) || !parse_i64(args[3], end))
              return encode_error("value is not an integer or out of range");
            if (args.size() >= 5) {
              auto unit = upper(args[4]);
              if (unit == "BIT") bit_mode = true;
              else if (unit != "BYTE") return encode_error("syntax error");
            }
          }
          if (bit_mode) {
            // Bit-level range
            std::int64_t total_bits = static_cast<std::int64_t>(s.size()) * 8;
            if (start < 0) start += total_bits;
            if (end < 0) end += total_bits;
            if (start < 0) start = 0;
            if (end >= total_bits) end = total_bits - 1;
            if (start > end) return encode_integer(0);
            long long count = 0;
            for (std::int64_t i = start; i <= end; ++i) {
              std::size_t byte_idx = static_cast<std::size_t>(i / 8);
              int bit_idx = 7 - static_cast<int>(i % 8);
              if (byte_idx < s.size() && ((static_cast<unsigned char>(s[byte_idx]) >> bit_idx) & 1)) ++count;
            }
            return encode_integer(count);
          }
          // Byte-level range
          std::int64_t len = static_cast<std::int64_t>(s.size());
          if (start < 0) start += len;
          if (end < 0) end += len;
          if (start < 0) start = 0;
          if (end >= len) end = len - 1;
          if (start > end) return encode_integer(0);
          long long count = 0;
          for (std::int64_t i = start; i <= end; ++i) {
            unsigned char byte = static_cast<unsigned char>(s[static_cast<std::size_t>(i)]);
            count += __builtin_popcount(byte);
          }
          return encode_integer(count);
        }});

    // ─── BITPOS ──────────────────────────────────────────────────────────
    t.emplace("BITPOS", CommandSpec{"BITPOS", -3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_string(args[1])) return wrongtype_error_reply();
          std::int64_t bit;
          if (!parse_i64(args[2], bit) || (bit != 0 && bit != 1))
            return encode_error("bit is not an integer or out of range");
          auto val = store().get(args[1]);
          std::string s = val ? *val : "";
          if (s.empty()) {
            if (bit == 0) return encode_integer(0);
            return encode_integer(-1);
          }
          std::int64_t start = 0, end = static_cast<std::int64_t>(s.size()) - 1;
          bool has_end = false;
          bool bit_mode = false;
          if (args.size() >= 4) { if (!parse_i64(args[3], start)) return encode_error("value is not an integer or out of range"); }
          if (args.size() >= 5) { if (!parse_i64(args[4], end)) return encode_error("value is not an integer or out of range"); has_end = true; }
          if (args.size() >= 6) {
            auto unit = upper(args[5]);
            if (unit == "BIT") bit_mode = true;
            else if (unit != "BYTE") return encode_error("syntax error");
          }
          std::int64_t len = static_cast<std::int64_t>(s.size());
          if (!bit_mode) {
            if (start < 0) start += len;
            if (end < 0) end += len;
            if (start < 0) start = 0;
            if (end >= len) end = len - 1;
            if (start > end) return encode_integer(-1);
            for (std::int64_t byte_i = start; byte_i <= end; ++byte_i) {
              unsigned char byte = static_cast<unsigned char>(s[static_cast<std::size_t>(byte_i)]);
              for (int bi = 7; bi >= 0; --bi) {
                if (((byte >> bi) & 1) == static_cast<unsigned>(bit)) {
                  return encode_integer(byte_i * 8 + (7 - bi));
                }
              }
            }
            if (bit == 0 && !has_end) return encode_integer(static_cast<long long>(s.size()) * 8);
            return encode_integer(-1);
          }
          // Bit mode
          std::int64_t total_bits = len * 8;
          if (start < 0) start += total_bits;
          if (end < 0) end += total_bits;
          if (start < 0) start = 0;
          if (end >= total_bits) end = total_bits - 1;
          if (start > end) return encode_integer(-1);
          for (std::int64_t i = start; i <= end; ++i) {
            std::size_t byte_idx = static_cast<std::size_t>(i / 8);
            int bit_idx = 7 - static_cast<int>(i % 8);
            int b = (static_cast<unsigned char>(s[byte_idx]) >> bit_idx) & 1;
            if (b == static_cast<int>(bit)) return encode_integer(i);
          }
          return encode_integer(-1);
        }});

    // ─── BITOP ───────────────────────────────────────────────────────────
    t.emplace("BITOP", CommandSpec{"BITOP", -4, {"write"}, 2, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          const auto op = upper(args[1]);
          if (op != "AND" && op != "OR" && op != "XOR" && op != "NOT")
            return encode_error("BITOP requires AND, OR, XOR or NOT operation");
          if (op == "NOT" && args.size() != 4)
            return encode_error("BITOP NOT requires one and only one key");
          // Gather source strings
          std::vector<std::string> sources;
          for (std::size_t i = 3; i < args.size(); ++i) {
            if (store().is_wrongtype_for_string(args[i])) return wrongtype_error_reply();
            auto val = store().get(args[i]);
            sources.push_back(val ? *val : "");
          }
          // Determine max length
          std::size_t maxlen = 0;
          for (const auto& s : sources) maxlen = std::max(maxlen, s.size());
          std::string result(maxlen, '\0');
          for (std::size_t i = 0; i < maxlen; ++i) {
            unsigned char byte = (i < sources[0].size()) ? static_cast<unsigned char>(sources[0][i]) : 0;
            if (op == "NOT") {
              result[i] = static_cast<char>(~byte);
            } else {
              for (std::size_t j = 1; j < sources.size(); ++j) {
                unsigned char b = (i < sources[j].size()) ? static_cast<unsigned char>(sources[j][i]) : 0;
                if (op == "AND") byte &= b;
                else if (op == "OR") byte |= b;
                else if (op == "XOR") byte ^= b;
              }
              result[i] = static_cast<char>(byte);
            }
          }
          store().set(args[2], result, false, false, std::nullopt, false);
          return encode_integer(static_cast<long long>(result.size()));
        }});

    // ─── BITFIELD ────────────────────────────────────────────────────────
    t.emplace("BITFIELD", CommandSpec{"BITFIELD", -2, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          // Simplified BITFIELD: supports GET, SET, INCRBY with unsigned/signed integer types
          if (store().is_wrongtype_for_string(args[1])) return wrongtype_error_reply();
          auto val = store().get(args[1]);
          std::string s = val ? *val : "";
          auto get_bit = [&](std::size_t bit_pos) -> int {
            std::size_t byte_idx = bit_pos / 8;
            int bit_idx = 7 - static_cast<int>(bit_pos % 8);
            if (byte_idx >= s.size()) return 0;
            return (static_cast<unsigned char>(s[byte_idx]) >> bit_idx) & 1;
          };
          auto set_bit_fn = [&](std::size_t bit_pos, int val_bit) {
            std::size_t byte_idx = bit_pos / 8;
            int bit_idx = 7 - static_cast<int>(bit_pos % 8);
            while (s.size() <= byte_idx) s.push_back('\0');
            if (val_bit) s[byte_idx] = static_cast<char>(static_cast<unsigned char>(s[byte_idx]) | (1 << bit_idx));
            else s[byte_idx] = static_cast<char>(static_cast<unsigned char>(s[byte_idx]) & ~(1 << bit_idx));
          };
          auto parse_type = [](const std::string& t, bool& is_signed, int& bits) -> bool {
            if (t.empty()) return false;
            is_signed = (t[0] == 'i');
            if (t[0] != 'i' && t[0] != 'u') return false;
            try { bits = std::stoi(t.substr(1)); return bits > 0 && bits <= 64; }
            catch (...) { return false; }
          };
          auto parse_offset = [](const std::string& s, int bits, std::size_t& off) -> bool {
            if (s.empty()) return false;
            if (s[0] == '#') {
              try { off = static_cast<std::size_t>(std::stoull(s.substr(1))) * static_cast<std::size_t>(bits); return true; }
              catch (...) { return false; }
            }
            try { off = static_cast<std::size_t>(std::stoull(s)); return true; }
            catch (...) { return false; }
          };
          auto read_field = [&](std::size_t offset, int bits, bool is_signed) -> std::int64_t {
            std::uint64_t v = 0;
            for (int i = 0; i < bits; ++i) {
              v = (v << 1) | static_cast<std::uint64_t>(get_bit(offset + static_cast<std::size_t>(i)));
            }
            if (is_signed && bits < 64 && (v & (1ULL << (bits - 1)))) {
              v |= ~((1ULL << bits) - 1);
            }
            return static_cast<std::int64_t>(v);
          };
          auto write_field = [&](std::size_t offset, int bits, std::int64_t value) {
            std::uint64_t v = static_cast<std::uint64_t>(value);
            for (int i = bits - 1; i >= 0; --i) {
              set_bit_fn(offset + static_cast<std::size_t>(bits - 1 - i), static_cast<int>((v >> i) & 1));
            }
          };
          bool modified = false;
          std::vector<std::string> results;
          std::size_t i = 2;
          while (i < args.size()) {
            auto subcmd = upper(args[i]);
            if (subcmd == "OVERFLOW") { ++i; if (i < args.size()) ++i; continue; } // skip overflow mode
            if (subcmd == "GET") {
              if (i + 2 >= args.size()) return encode_error("syntax error");
              bool is_signed; int bits;
              if (!parse_type(args[i + 1], is_signed, bits)) return encode_error("Invalid bitfield type");
              std::size_t off;
              if (!parse_offset(args[i + 2], bits, off)) return encode_error("bit offset is not an integer or out of range");
              results.push_back(encode_integer(read_field(off, bits, is_signed)));
              i += 3;
            } else if (subcmd == "SET") {
              if (i + 3 >= args.size()) return encode_error("syntax error");
              bool is_signed; int bits;
              if (!parse_type(args[i + 1], is_signed, bits)) return encode_error("Invalid bitfield type");
              std::size_t off;
              if (!parse_offset(args[i + 2], bits, off)) return encode_error("bit offset is not an integer or out of range");
              std::int64_t new_val;
              if (!parse_i64(args[i + 3], new_val)) return encode_error("value is not an integer or out of range");
              results.push_back(encode_integer(read_field(off, bits, is_signed)));
              write_field(off, bits, new_val);
              modified = true;
              i += 4;
            } else if (subcmd == "INCRBY") {
              if (i + 3 >= args.size()) return encode_error("syntax error");
              bool is_signed; int bits;
              if (!parse_type(args[i + 1], is_signed, bits)) return encode_error("Invalid bitfield type");
              std::size_t off;
              if (!parse_offset(args[i + 2], bits, off)) return encode_error("bit offset is not an integer or out of range");
              std::int64_t incr;
              if (!parse_i64(args[i + 3], incr)) return encode_error("value is not an integer or out of range");
              std::int64_t old_val = read_field(off, bits, is_signed);
              std::int64_t new_val = old_val + incr;
              write_field(off, bits, new_val);
              results.push_back(encode_integer(read_field(off, bits, is_signed)));
              modified = true;
              i += 4;
            } else {
              return encode_error("syntax error");
            }
          }
          if (modified) {
            store().set(args[1], s, false, false, std::nullopt, true);
          }
          return encode_array(results);
        }});

    // ─── GEODIST ─────────────────────────────────────────────────────────
    t.emplace("GEODIST", CommandSpec{"GEODIST", -4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          // Geo data is stored as zset with geohash as score
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          // Decode 52-bit interleaved geohash back to lon/lat.
          auto decode_geohash = [](double score, double& lon, double& lat) {
            auto bits = static_cast<std::uint64_t>(score);
            double lon_min = -180, lon_max = 180;
            double lat_min = -90, lat_max = 90;
            for (int i = 51; i >= 0; --i) {
              int b = (bits >> i) & 1;
              if (i % 2 == 1) { // longitude bits (even positions from MSB = odd i)
                double mid = (lon_min + lon_max) / 2;
                if (b) lon_min = mid; else lon_max = mid;
              } else { // latitude bits
                double mid = (lat_min + lat_max) / 2;
                if (b) lat_min = mid; else lat_max = mid;
              }
            }
            lon = (lon_min + lon_max) / 2;
            lat = (lat_min + lat_max) / 2;
          };
          double lon1 = 0, lat1 = 0, lon2 = 0, lat2 = 0;
          bool found1 = false, found2 = false;
          for (const auto& [m, s] : items) {
            if (m == args[2]) { decode_geohash(s, lon1, lat1); found1 = true; }
            if (m == args[3]) { decode_geohash(s, lon2, lat2); found2 = true; }
          }
          if (!found1 || !found2) return encode_null(session.resp_version);
          // Haversine distance
          constexpr double kEarthRadius = 6372797.560856;
          constexpr double kPi = 3.14159265358979323846;
          double lat1r = lat1 * kPi / 180.0, lat2r = lat2 * kPi / 180.0;
          double dlat = (lat2 - lat1) * kPi / 180.0;
          double dlon = (lon2 - lon1) * kPi / 180.0;
          double a = std::sin(dlat / 2) * std::sin(dlat / 2) + std::cos(lat1r) * std::cos(lat2r) * std::sin(dlon / 2) * std::sin(dlon / 2);
          double c = 2 * std::atan2(std::sqrt(a), std::sqrt(1 - a));
          double dist = kEarthRadius * c;
          // Unit conversion
          std::string unit = "m";
          if (args.size() >= 5) unit = lower(args[4]);
          if (unit == "km") dist /= 1000.0;
          else if (unit == "mi") dist /= 1609.34;
          else if (unit == "ft") dist /= 0.3048;
          std::ostringstream oss;
          oss << std::fixed << std::setprecision(4) << dist;
          return encode_bulk(oss.str());
        }});

    // ─── GEOPOS ──────────────────────────────────────────────────────────
    t.emplace("GEOPOS", CommandSpec{"GEOPOS", -3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          // Decode 52-bit interleaved geohash back to lon/lat.
          auto decode_geohash = [](double score, double& lon, double& lat) {
            auto bits = static_cast<std::uint64_t>(score);
            double lon_min = -180, lon_max = 180;
            double lat_min = -90, lat_max = 90;
            for (int i = 51; i >= 0; --i) {
              int b = (bits >> i) & 1;
              if (i % 2 == 1) { // longitude bits (even positions from MSB = odd i)
                double mid = (lon_min + lon_max) / 2;
                if (b) lon_min = mid; else lon_max = mid;
              } else { // latitude bits
                double mid = (lat_min + lat_max) / 2;
                if (b) lat_min = mid; else lat_max = mid;
              }
            }
            lon = (lon_min + lon_max) / 2;
            lat = (lat_min + lat_max) / 2;
          };
          std::unordered_map<std::string, std::pair<double, double>> coords;
          for (const auto& [m, s] : items) {
            double lon, lat;
            decode_geohash(s, lon, lat);
            coords[m] = {lon, lat};
          }
          std::vector<std::string> enc;
          for (std::size_t i = 2; i < args.size(); ++i) {
            auto it = coords.find(args[i]);
            if (it == coords.end()) {
              enc.push_back(encode_null_array());
            } else {
              std::ostringstream oss_lon, oss_lat;
              oss_lon << std::fixed << std::setprecision(6) << it->second.first;
              oss_lat << std::fixed << std::setprecision(6) << it->second.second;
              enc.push_back(encode_array({encode_bulk(oss_lon.str()), encode_bulk(oss_lat.str())}));
            }
          }
          return encode_array(enc);
        }});

    // ─── GEOHASH ─────────────────────────────────────────────────────────
    t.emplace("GEOHASH", CommandSpec{"GEOHASH", -3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          // Decode 52-bit interleaved geohash back to lon/lat.
          auto decode_geohash = [](double score, double& lon, double& lat) {
            auto bits = static_cast<std::uint64_t>(score);
            double lon_min = -180, lon_max = 180;
            double lat_min = -90, lat_max = 90;
            for (int i = 51; i >= 0; --i) {
              int b = (bits >> i) & 1;
              if (i % 2 == 1) { // longitude bits (even positions from MSB = odd i)
                double mid = (lon_min + lon_max) / 2;
                if (b) lon_min = mid; else lon_max = mid;
              } else { // latitude bits
                double mid = (lat_min + lat_max) / 2;
                if (b) lat_min = mid; else lat_max = mid;
              }
            }
            lon = (lon_min + lon_max) / 2;
            lat = (lat_min + lat_max) / 2;
          };
          constexpr const char* kBase32 = "0123456789bcdefghjkmnpqrstuvwxyz";
          auto encode_geohash_str = [&](double lon, double lat) -> std::string {
            double lon_min = -180, lon_max = 180;
            double lat_min = -90, lat_max = 90;
            bool is_lon = true;
            std::uint64_t bits = 0;
            int num_bits = 0;
            std::string hash;
            while (hash.size() < 11) {
              if (is_lon) {
                double mid = (lon_min + lon_max) / 2;
                if (lon >= mid) { bits = (bits << 1) | 1; lon_min = mid; }
                else { bits <<= 1; lon_max = mid; }
              } else {
                double mid = (lat_min + lat_max) / 2;
                if (lat >= mid) { bits = (bits << 1) | 1; lat_min = mid; }
                else { bits <<= 1; lat_max = mid; }
              }
              is_lon = !is_lon;
              ++num_bits;
              if (num_bits == 5) {
                hash += kBase32[bits & 0x1f];
                bits = 0;
                num_bits = 0;
              }
            }
            return hash;
          };
          std::unordered_map<std::string, double> score_map;
          for (const auto& [m, s] : items) score_map[m] = s;
          std::vector<std::string> enc;
          for (std::size_t i = 2; i < args.size(); ++i) {
            auto it = score_map.find(args[i]);
            if (it == score_map.end()) {
              enc.push_back(encode_null(session.resp_version));
            } else {
              double lon, lat;
              decode_geohash(it->second, lon, lat);
              enc.push_back(encode_bulk(encode_geohash_str(lon, lat)));
            }
          }
          return encode_array(enc);
        }});

    // ─── GEORADIUS (deprecated but widely used) ──────────────────────────
    t.emplace("GEORADIUS", CommandSpec{"GEORADIUS", -6, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          // Simplified: return empty array for now (no actual geo computation)
          // Full implementation would need geohash search
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          return encode_array({});
        }});

    t.emplace("GEORADIUSBYMEMBER", CommandSpec{"GEORADIUSBYMEMBER", -5, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          return encode_array({});
        }});

    // ─── GEOSEARCH ───────────────────────────────────────────────────────
    t.emplace("GEOSEARCH", CommandSpec{"GEOSEARCH", -7, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          // Simplified stub — full implementation requires geohash range scan
          return encode_array({});
        }});

    t.emplace("GEOSEARCHSTORE", CommandSpec{"GEOSEARCHSTORE", -8, {"write"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[2])) return wrongtype_error_reply();
          return encode_integer(0);
        }});

    // ─── XTRIM ───────────────────────────────────────────────────────────
    t.emplace("XTRIM", CommandSpec{"XTRIM", -4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_stream(args[1])) return wrongtype_error_reply();
          // Simplified: for MAXLEN, trim the stream
          bool wt = false;
          auto entries = store().xrange(args[1], "-", "+", false, wt);
          if (wt) return wrongtype_error_reply();
          const auto strategy = upper(args[2]);
          if (strategy == "MAXLEN") {
            std::size_t start = 3;
            if (start < args.size() && (args[start] == "~" || args[start] == "=")) ++start;
            if (start >= args.size()) return encode_error("syntax error");
            std::int64_t maxlen;
            if (!parse_i64(args[start], maxlen)) return encode_error("value is not an integer or out of range");
            if (maxlen < 0) return encode_error("The MAXLEN argument must be >= 0.");
            long long trimmed = 0;
            if (static_cast<std::int64_t>(entries.size()) > maxlen) {
              std::size_t to_remove = entries.size() - static_cast<std::size_t>(maxlen);
              std::vector<std::string> ids;
              for (std::size_t i = 0; i < to_remove; ++i) ids.push_back(entries[i].first);
              trimmed = store().xdel(args[1], ids, wt);
            }
            return encode_integer(trimmed);
          }
          if (strategy == "MINID") {
            std::size_t start = 3;
            if (start < args.size() && (args[start] == "~" || args[start] == "=")) ++start;
            if (start >= args.size()) return encode_error("syntax error");
            const auto& min_id = args[start];
            long long trimmed = 0;
            std::vector<std::string> ids;
            for (const auto& [id, _] : entries) {
              if (id < min_id) ids.push_back(id);
            }
            if (!ids.empty()) trimmed = store().xdel(args[1], ids, wt);
            return encode_integer(trimmed);
          }
          return encode_error("syntax error");
        }});

    // ─── XAUTOCLAIM ──────────────────────────────────────────────────────
    t.emplace("XAUTOCLAIM", CommandSpec{"XAUTOCLAIM", -6, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          if (store().is_wrongtype_for_stream(args[1])) return wrongtype_error_reply();
          // Stub: return empty results (next-cursor, claimed-entries, deleted-ids)
          return encode_array({encode_bulk("0-0"), encode_array({}), encode_array({})});
        }});

    // Pre-compute flag bitmasks for fast checking
    for (auto& [key, spec] : t) {
      spec.flag_bits = 0;
      for (const auto& f : spec.flags) {
        if (f == "write") spec.flag_bits |= kFlagWrite;
        else if (f == "readonly") spec.flag_bits |= kFlagReadonly;
        else if (f == "fast") spec.flag_bits |= kFlagFast;
        else if (f == "admin") spec.flag_bits |= kFlagAdmin;
        else if (f == "noscript") spec.flag_bits |= kFlagNoScript;
        else if (f == "pubsub") spec.flag_bits |= kFlagPubSub;
        else if (f == "loading") spec.flag_bits |= kFlagLoading;
        else if (f == "stale") spec.flag_bits |= kFlagStale;
        else if (f == "no_auth") spec.flag_bits |= kFlagNoAuth;
      }
    }

    return t;
  }();

  return table;
}

} // namespace

std::string handle_command(const std::vector<std::string>& args, SessionState& session, bool& should_close) {
  if (args.empty() || args[0] == "__empty__" || args[0] == "__parse_error__") {
    return encode_error("Protocol error");
  }

  auto& ds = store();
  if (session.db_index != ds.current_db()) {
    ds.select_db(session.db_index);
  }

  // Upper-case the command name in-place using a thread-local buffer to avoid allocation
  thread_local std::string cmd_buf;
  cmd_buf.assign(args[0]);
  for (auto& c : cmd_buf) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  const std::string& cmd = cmd_buf;
  const bool is_script_cmd =
      (cmd == "EVAL" || cmd == "EVALSHA" || cmd == "EVAL_RO" || cmd == "EVALSHA_RO" ||
       cmd == "FCALL" || cmd == "FCALL_RO");
  if (is_script_cmd) {
    g_script_inner_error_seen = false;
  }
  const auto& table = command_table();
  const bool is_script_kill =
      (cmd == "SCRIPT" && args.size() == 2 && upper(args[1]) == "KILL") ||
      (cmd == "FUNCTION" && args.size() == 2 && upper(args[1]) == "KILL");
  if (g_script_busy.load() && !is_script_kill && cmd != "SHUTDOWN") {
    if (cmd == "PING" && g_busy_script_session == &session) {
      return args.size() == 2 ? encode_bulk(args[1]) : encode_simple("PONG");
    }
    if (!session.in_multi && cmd == "MULTI") {
      // Redis allows entering MULTI while busy; queued commands can still fail with BUSY.
    } else {
    if (session.in_multi && cmd == "EXEC") {
      session.in_multi = false;
      session.multi_dirty = false;
      session.queued.clear();
      session.watched_epoch.reset();
      session.watched_digests.clear();
      return "-EXECABORT Transaction discarded because of previous errors: BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n";
    }
    if (session.in_multi && cmd != "DISCARD" && cmd != "MULTI" && cmd != "QUIT") {
      session.multi_dirty = true;
    }
    return busy_script_error_reply();
    }
  }
  if (session.in_multi && cmd != "EXEC" && cmd != "DISCARD" && cmd != "MULTI" && cmd != "QUIT") {
    if (cmd == "WATCH") return encode_error("WATCH inside MULTI is not allowed");
    if (cmd == "SAVE" || cmd == "SHUTDOWN") {
      session.multi_dirty = true;
      return std::string("-ERR Command not allowed inside a transaction\r\n");
    }
    const auto qit = table.find(cmd);
    if (qit == table.end()) {
      session.multi_dirty = true;
      return encode_error("unknown command");
    }
    if (!arity_ok(args.size(), qit->second.arity)) {
      session.multi_dirty = true;
      return encode_error("wrong number of arguments for '" + lower(cmd) + "' command");
    }
    bool q_is_write = qit->second.is_write();
    if (q_is_write && g_config_maxmemory == 1 && !g_script_allow_oom &&
        cmd != "EVAL" && cmd != "EVALSHA" && cmd != "FLUSHALL" && cmd != "FLUSHDB") {
      session.multi_dirty = true;
      return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    }
    session.queued.push_back(args);
    return encode_simple("QUEUED");
  }

  const auto it = table.find(cmd);
  if (it == table.end()) {
    auto mit = g_module_commands.find(cmd);
    if (mit == g_module_commands.end()) return encode_error("unknown command");
    std::vector<void*> argv;
    argv.reserve(args.size());
    for (const auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
    ModuleCallCtx ctx;
    (void)mit->second(&ctx, argv.data(), static_cast<int>(argv.size()));
    return ctx.replied ? ctx.reply : encode_null(RespVersion::Resp2);
  }

  const auto& spec = it->second;
  if (!arity_ok(args.size(), spec.arity)) {
    return encode_error("wrong number of arguments for '" + lower(cmd) + "' command");
  }
  auto& top_stat = g_cmdstats[lower(cmd)];
  const bool spec_is_write = spec.is_write();
  if (spec_is_write && g_config_maxmemory == 1 && !g_script_allow_oom &&
      cmd != "CONFIG" && cmd != "EVAL" && cmd != "EVALSHA" &&
      cmd != "FLUSHALL" && cmd != "FLUSHDB") {
    ++top_stat.rejected_calls;
    record_error_stat("OOM");
    if (in_lua_script_context()) g_script_inner_error_seen = true;
    return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
  }
  if (spec_is_write && g_config_min_replicas_to_write > 0 &&
      cmd != "EVAL" && cmd != "EVALSHA" &&
      cmd != "CONFIG" && cmd != "REPLICAOF" && cmd != "SLAVEOF") {
    ++top_stat.rejected_calls;
    record_error_stat("NOREPLICAS");
    if (in_lua_script_context()) g_script_inner_error_seen = true;
    return std::string("-NOREPLICAS Not enough good replicas to write.\r\n");
  }
  if (!spec_is_write && g_replication_role == "slave" && !g_config_replica_serve_stale_data &&
      g_replica_stale && cmd != "REPLICAOF" && cmd != "SLAVEOF" && cmd != "INFO" &&
      cmd != "MULTI" && cmd != "EXEC" && cmd != "DISCARD" &&
      cmd != "COMMAND" && cmd != "CONFIG") {
    return masterdown_stale_reply();
  }
  if (spec_is_write && g_replication_role == "slave" && !g_executing_exec &&
      !g_loading_replication.load() && !g_replaying_aof &&
      cmd != "REPLICAOF" && cmd != "SLAVEOF" && cmd != "PUBLISH") {
    ++top_stat.rejected_calls;
    record_error_stat("READONLY");
    if (in_lua_script_context()) g_script_inner_error_seen = true;
    return std::string("-READONLY You can't write against a read only replica.\r\n");
  }

  ++top_stat.calls;

  if (spec.first_key > 0 && static_cast<std::size_t>(spec.first_key) < args.size()) {
    const auto slot = cluster_keyslot(args[static_cast<std::size_t>(spec.first_key)]);
    const auto route = g_slot_routes[static_cast<std::size_t>(slot)];
    if (route == SlotRoute::Moved) {
      return "-MOVED " + std::to_string(slot) + " " + g_cluster_redirect_addr + "\r\n";
    }
    if (route == SlotRoute::Ask) {
      if (session.asking) {
        session.asking = false;
      } else {
        return "-ASK " + std::to_string(slot) + " " + g_cluster_redirect_addr + "\r\n";
      }
    }
  }
  if (cmd != "ASKING") {
    session.asking = false;
  }

  const std::string reply = spec.handler(args, session, should_close);
  if (is_error_reply(reply)) {
    ++top_stat.failed_calls;
    if (in_lua_script_context() && !is_script_cmd) {
      g_script_inner_error_seen = true;
    }
    const bool redis_inner_error = is_script_cmd ? (g_script_inner_error_seen || lua_engine_consume_last_error_from_redis()) : false;
    if (!is_script_cmd || !redis_inner_error) {
      record_error_stat(error_code_from_reply(reply));
    }
  }
  if (spec_is_write && !is_error_reply(reply)) {
    store().increment_mutation_epoch();
    // g_master_repl_offset is updated inside append_replication_event → backlog_append
    // via g_master_repl_offset = g_repl_backlog.total (absolute set, not increment).
    append_replication_event(args, session, &reply);
    append_aof(args);
  }
  return reply;
}

void configure_persistence(const std::string& dir, const std::string& dbfilename, bool appendonly,
                           const std::string& appendfilename, const std::string& appendfsync) {
  g_snapshot_path = std::filesystem::path(dir) / dbfilename;
  g_appendonly = appendonly;
  g_aof_path = std::filesystem::path(dir) / appendfilename;
  g_appendfsync = appendfsync;
  if (g_appendonly) open_aof_fd();
}

bool load_aof_file(const std::string& path, std::string& err) {
  err.clear();
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    err = "cannot open AOF";
    return false;
  }
  std::string buffer((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  std::size_t pos = 0;
  SessionState session {};
  g_replaying_aof = true;
  while (pos < buffer.size()) {
    auto parsed = parse_one_command(std::string_view(buffer).substr(pos));
    if (!parsed.has_value()) break;
    bool close = false;
    const auto reply = handle_command(parsed->args, session, close);
    if (is_error_reply(reply)) {
      g_replaying_aof = false;
      err = "AOF replay command failed";
      return false;
    }
    pos += parsed->consumed;
  }
  g_replaying_aof = false;
  return true;
}

void configure_runtime_port(int port) {
  g_server_port = port;
}

void configure_max_clients(int max_clients) {
  if (max_clients < 1) return;
  g_runtime_max_clients.store(max_clients, std::memory_order_relaxed);
}

void set_active_expire_enabled(bool enabled) {
  g_active_expire = enabled;
}

bool active_expire_enabled() {
  return g_active_expire;
}

void set_lua_script_context(bool in_script) {
  g_in_lua_script = in_script;
}

bool in_lua_script_context() {
  return g_in_lua_script;
}

void record_lua_script_rejected_write(const std::string& cmd) {
  auto& st = g_cmdstats[lower(cmd)];
  ++st.rejected_calls;
  record_error_stat("ERR");
  g_script_inner_error_seen = true;
}

static std::atomic<bool> g_shutdown_requested{false};

void request_shutdown() {
  g_shutdown_requested.store(true, std::memory_order_release);
}

bool shutdown_requested() {
  return g_shutdown_requested.load(std::memory_order_acquire);
}

int configured_max_clients() {
  return g_runtime_max_clients.load(std::memory_order_relaxed);
}

std::size_t replication_event_count() {
  return static_cast<std::size_t>(g_repl_backlog.total);
}

// Returns all backlog bytes from byte-offset `from` to current total.
std::string replication_event_at(std::size_t from) {
  std::string out;
  g_repl_backlog.get_slice(static_cast<std::int64_t>(from), out);
  return out;
}

// ─── Blocking client support ──────────────────────────────────────────────

std::string try_unblock_client(SessionState& session) {
  if (session.blocked.type == BlockType::None) return {};

  const auto current = now_ms();

  // Check timeout
  if (session.blocked.deadline_ms > 0 && current >= session.blocked.deadline_ms) {
    unregister_key_waiter(session.fd);
    session.blocked.type = BlockType::None;
    session.blocked.keys.clear();
    session.blocked.original_args.clear();
    return encode_null(RespVersion::Resp2);
  }

  if (session.blocked.type == BlockType::List) {
    // Check if this is a BLMOVE-style command
    const auto& orig = session.blocked.original_args;
    bool is_blmove = (!orig.empty() && upper(orig[0]) == "BLMOVE");

    for (const auto& key : session.blocked.keys) {
      bool wrongtype = false;
      std::optional<std::string> val;
      if (session.blocked.pop_left) {
        val = store().lpop(key, wrongtype);
      } else {
        val = store().rpop(key, wrongtype);
      }
      if (wrongtype) {
        unregister_key_waiter(session.fd);
        session.blocked.type = BlockType::None;
        session.blocked.keys.clear();
        session.blocked.original_args.clear();
        return wrongtype_error_reply();
      }
      if (val.has_value()) {
        unregister_key_waiter(session.fd);
        session.blocked.type = BlockType::None;
        session.blocked.keys.clear();

        if (is_blmove && orig.size() >= 5) {
          // BLMOVE src dst wherefrom whereto timeout
          const std::string dst = orig[2];
          const std::string where_to = upper(orig[4]);
          std::vector<std::string> one{*val};
          bool wt = false;
          if (where_to == "LEFT") (void)store().lpush(dst, one, wt);
          else (void)store().rpush(dst, one, wt);
          session.blocked.original_args.clear();
          return encode_bulk(*val);
        }

        session.blocked.original_args.clear();
        return encode_array({encode_bulk(key), encode_bulk(*val)});
      }
    }
  } else if (session.blocked.type == BlockType::ZSet) {
    for (const auto& key : session.blocked.keys) {
      bool wrongtype = false;
      auto vals = store().zpop(key, 1, session.blocked.zpop_max, wrongtype);
      if (wrongtype) {
        unregister_key_waiter(session.fd);
        session.blocked.type = BlockType::None;
        session.blocked.keys.clear();
        session.blocked.original_args.clear();
        return wrongtype_error_reply();
      }
      if (!vals.empty()) {
        unregister_key_waiter(session.fd);
        session.blocked.type = BlockType::None;
        session.blocked.keys.clear();
        session.blocked.original_args.clear();
        std::ostringstream oss;
        oss << vals[0].second;
        return encode_array({encode_bulk(key), encode_bulk(vals[0].first), encode_bulk(oss.str())});
      }
    }
  }

  return {}; // still blocked
}

// Signals that a key was written — the server loop checks this flag to know
// it should re-evaluate blocked clients on the next tick.
static bool g_has_pending_wakeups = false;

void register_key_waiter(const std::string& /*key*/, int /*fd*/) {}
void unregister_key_waiter(int /*fd*/) {}

bool has_pending_wakeups() { return g_has_pending_wakeups; }
void clear_pending_wakeups() { g_has_pending_wakeups = false; }

void notify_key_written(const std::string& /*key*/, int /*db_index*/) {
  g_has_pending_wakeups = true;
}

std::size_t command_count() { return command_table().size(); }

std::vector<std::string> command_names() {
  std::vector<std::string> names;
  names.reserve(command_table().size());
  for (const auto& [name, _] : command_table()) names.push_back(name);
  std::sort(names.begin(), names.end());
  return names;
}

void aof_fsync_if_needed(std::int64_t now) {
  if (!g_appendonly || g_aof_fd < 0 || g_appendfsync != "everysec") return;
  if (now - g_last_aof_fsync_ms >= 1000) {
    ::fsync(g_aof_fd);
    g_last_aof_fsync_ms = now;
  }
}

// ── In-process replication client helpers ────────────────────────────────────

static bool repl_readline(int fd, std::string& out) {
    out.clear();
    char c;
    while (true) {
        ssize_t n = ::read(fd, &c, 1);
        if (n <= 0) return false;
        if (c == '\n') {
            if (!out.empty() && out.back() == '\r') out.pop_back();
            return true;
        }
        out += c;
        if (out.size() > 65536) return false;
    }
}

static bool repl_read_exact(int fd, void* buf, std::size_t len) {
    auto* p = static_cast<char*>(buf);
    std::size_t done = 0;
    while (done < len) {
        ssize_t n = ::read(fd, p + done, len - done);
        if (n <= 0) return false;
        done += static_cast<std::size_t>(n);
    }
    return true;
}

static bool repl_send(int fd, const std::string& data) {
    std::size_t done = 0;
    while (done < data.size()) {
        ssize_t n = ::write(fd, data.c_str() + done, data.size() - done);
        if (n <= 0) return false;
        done += static_cast<std::size_t>(n);
    }
    return true;
}

static std::string repl_encode_cmd(const std::vector<std::string>& parts) {
    std::string out = "*" + std::to_string(parts.size()) + "\r\n";
    for (const auto& s : parts)
        out += "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
    return out;
}

// Perform a full PSYNC handshake with master at host:port and load the RDB snapshot.
bool replica_do_fullsync(const std::string& host, int port) {
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    const std::string port_str = std::to_string(port);
    if (::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0 || !res) {
        log(LogLevel::Error, "REPLICAOF: cannot resolve " + host + ":" + port_str);
        return false;
    }

    int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        ::freeaddrinfo(res);
        log(LogLevel::Error, "REPLICAOF: socket() failed");
        return false;
    }
    if (::connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        log(LogLevel::Error, "REPLICAOF: connect to " + host + ":" + port_str + " failed: " + strerror(errno));
        ::freeaddrinfo(res);
        ::close(fd);
        return false;
    }
    ::freeaddrinfo(res);

    std::string line;

    // PING
    if (!repl_send(fd, "*1\r\n$4\r\nPING\r\n") ||
        !repl_readline(fd, line) ||
        (line != "+PONG" && line.rfind("+PONG", 0) != 0)) {
        log(LogLevel::Error, "REPLICAOF: PING failed, got: " + line);
        ::close(fd);
        return false;
    }

    // REPLCONF listening-port
    if (!repl_send(fd, repl_encode_cmd({"REPLCONF", "listening-port", std::to_string(g_server_port)})) ||
        !repl_readline(fd, line) || line.rfind("+OK", 0) != 0) {
        log(LogLevel::Error, "REPLICAOF: REPLCONF listening-port failed: " + line);
        ::close(fd);
        return false;
    }

    // REPLCONF capa psync2
    if (!repl_send(fd, repl_encode_cmd({"REPLCONF", "capa", "psync2"})) ||
        !repl_readline(fd, line) || line.rfind("+OK", 0) != 0) {
        log(LogLevel::Error, "REPLICAOF: REPLCONF capa failed: " + line);
        ::close(fd);
        return false;
    }

    // PSYNC ? -1 (request full resync)
    if (!repl_send(fd, repl_encode_cmd({"PSYNC", "?", "-1"})) ||
        !repl_readline(fd, line)) {
        ::close(fd);
        return false;
    }
    if (line.rfind("+FULLRESYNC", 0) != 0) {
        log(LogLevel::Error, "REPLICAOF: expected +FULLRESYNC, got: " + line);
        ::close(fd);
        return false;
    }
    // Parse replid and offset: "+FULLRESYNC <replid> <offset>"
    {
        auto sp1 = line.find(' ');
        auto sp2 = line.find(' ', sp1 + 1);
        if (sp1 != std::string::npos && sp2 != std::string::npos) {
            g_master_replid = line.substr(sp1 + 1, sp2 - sp1 - 1);
            std::int64_t off = 0;
            parse_i64(line.substr(sp2 + 1), off);
            g_master_repl_offset = off;
        }
    }

    // Read $<N>\r\n  (may be preceded by a stray \n from some masters)
    if (!repl_readline(fd, line)) { ::close(fd); return false; }
    if (line.empty()) {
        if (!repl_readline(fd, line)) { ::close(fd); return false; }
    }
    if (line.empty() || line[0] != '$') {
        log(LogLevel::Error, "REPLICAOF: expected $<len>, got: " + line);
        ::close(fd);
        return false;
    }
    std::int64_t rdb_len = 0;
    if (!parse_i64(line.substr(1), rdb_len) || rdb_len < 0) {
        log(LogLevel::Error, "REPLICAOF: bad RDB length: " + line);
        ::close(fd);
        return false;
    }

    // Stream RDB into a temp file then load it
    const std::string tmp_path = "/tmp/peadb_fullsync_" + std::to_string(::getpid()) + ".rdb.tmp";
    {
        int rfd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
        if (rfd < 0) {
            log(LogLevel::Error, "REPLICAOF: cannot open temp file " + tmp_path + ": " + strerror(errno));
            ::close(fd);
            return false;
        }
        std::vector<char> chunk(65536);
        std::int64_t remaining = rdb_len;
        bool write_ok = true;
        while (remaining > 0 && write_ok) {
            std::size_t want = static_cast<std::size_t>(std::min(remaining, static_cast<std::int64_t>(chunk.size())));
            if (!repl_read_exact(fd, chunk.data(), want)) {
                write_ok = false;
                break;
            }
            if (::write(rfd, chunk.data(), want) != static_cast<ssize_t>(want)) {
                write_ok = false;
                break;
            }
            remaining -= static_cast<std::int64_t>(want);
        }
        ::close(rfd);
        if (!write_ok) {
            ::unlink(tmp_path.c_str());
            ::close(fd);
            log(LogLevel::Error, "REPLICAOF: failed streaming RDB data");
            return false;
        }
    }
    // Note: keep fd open for command streaming after RDB load.

    std::string err;
    if (!rdb_load(tmp_path, err)) {
        ::unlink(tmp_path.c_str());
        ::close(fd);
        log(LogLevel::Error, "REPLICAOF: rdb_load failed: " + err);
        return false;
    }
    ::unlink(tmp_path.c_str());
    log(LogLevel::Info, "REPLICAOF: full sync complete, loaded " + std::to_string(rdb_len) + " bytes");

    // Release loading gate so client writes are now rejected by the slave readonly guard.
    // The streaming loop uses g_replaying_aof to bypass the guard for master commands.
    g_loading_replication.store(false);
    g_replica_stale = false;

    // Stream subsequent replication commands from master.
    // The master sends a continuous RESP stream of write commands after the RDB.
    // We parse each RESP array and apply it via handle_command with AOF/backlog suppressed.
    std::string stream_buf;
    stream_buf.reserve(65536);
    std::vector<char> recv_buf(65536);
    bool close_flag = false;
    SessionState repl_session;
    repl_session.db_index = 0;

    // Local counter tracks bytes received from master for REPLCONF ACK.
    // g_master_repl_offset is the local write-backlog cursor and must not be
    // conflated with the replica-side receive position.
    std::int64_t replica_rx_bytes = 0;

    // Set recv timeout once before the loop so we can send periodic ACKs.
    {
        struct timeval tv{1, 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    auto last_ack_ms = now_ms();
    while (true) {
        // Non-blocking check: send REPLCONF ACK every ~1s
        const auto now = now_ms();
        if (now - last_ack_ms >= 1000) {
            const std::string ack = repl_encode_cmd({"REPLCONF", "ACK", std::to_string(replica_rx_bytes)});
            repl_send(fd, ack);
            last_ack_ms = now;
        }

        const ssize_t n = ::recv(fd, recv_buf.data(), recv_buf.size(), 0);
        if (n == 0) {
            log(LogLevel::Warn, "REPLICAOF: master closed connection");
            break;
        }
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) continue;
            log(LogLevel::Error, "REPLICAOF: recv error: " + std::string(strerror(errno)));
            break;
        }
        stream_buf.append(recv_buf.data(), static_cast<std::size_t>(n));

        // Parse and apply all complete RESP commands in the buffer.
        // g_replaying_aof suppresses AOF writes and backlog updates (append_replication_event).
        g_replaying_aof = true;
        std::size_t consumed = 0;
        while (true) {
            const std::string_view view(stream_buf.data() + consumed, stream_buf.size() - consumed);
            auto parsed = parse_one_command(view);
            if (!parsed.has_value()) break;
            handle_command(parsed->args, repl_session, close_flag);
            consumed += parsed->consumed;
            replica_rx_bytes += static_cast<std::int64_t>(parsed->consumed);
        }
        g_replaying_aof = false;

        if (consumed > 0) {
            if (consumed == stream_buf.size()) stream_buf.clear();
            else stream_buf.erase(0, consumed);
        }
    }
    ::close(fd);
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────

void reap_background_children() {
  if (g_bgsave_in_progress.load()) {
    int status = 0;
    pid_t pid = g_bgsave_child_pid.load();
    if (pid > 0 && ::waitpid(pid, &status, WNOHANG) == pid) {
      g_last_bgsave_status = (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? "ok" : "err";
      g_last_save_time = now_ms() / 1000;
      g_bgsave_child_pid.store(0);
      g_bgsave_in_progress.store(false);
    }
  }
  if (g_bgrewrite_in_progress.load()) {
    int status = 0;
    pid_t pid = g_bgrewrite_child_pid.load();
    if (pid > 0 && ::waitpid(pid, &status, WNOHANG) == pid) {
      g_bgrewrite_child_pid.store(0);
      g_bgrewrite_in_progress.store(false);
      // The child renamed the temp AOF into place. Append any commands that
      // arrived during the rewrite window, then reopen the persistent fd.
      if (WIFEXITED(status) && WEXITSTATUS(status) == 0 && g_appendonly) {
        if (!g_aof_rewrite_buf.empty()) {
          const int diff_fd = ::open(g_aof_path.c_str(),
                                     O_WRONLY | O_APPEND | O_CLOEXEC, 0644);
          bool diff_ok = false;
          if (diff_fd >= 0) {
            const char* p = g_aof_rewrite_buf.data();
            std::size_t remaining = g_aof_rewrite_buf.size();
            diff_ok = true;
            while (remaining > 0) {
              const ssize_t rc = ::write(diff_fd, p, remaining);
              if (rc <= 0) { diff_ok = false; break; }
              p += static_cast<std::size_t>(rc);
              remaining -= static_cast<std::size_t>(rc);
            }
            if (diff_ok && ::fsync(diff_fd) != 0) diff_ok = false;
            ::close(diff_fd);
          }
          if (diff_ok) {
            g_aof_rewrite_buf.clear();
          } else {
            log(LogLevel::Error, "BGREWRITEAOF: failed to append rewrite diff (" +
                std::to_string(g_aof_rewrite_buf.size()) + " bytes); AOF may be incomplete");
            // Buffer not cleared — next successful BGREWRITEAOF can retry.
          }
        }
        if (g_aof_rewrite_buf.empty()) open_aof_fd();
      } else {
        if (!g_aof_rewrite_buf.empty()) {
          log(LogLevel::Warn, "BGREWRITEAOF: child failed; discarding " +
              std::to_string(g_aof_rewrite_buf.size()) + " bytes of buffered diff");
        }
        g_aof_rewrite_buf.clear();
      }
    }
  }
}

// ── ServerContext accessor ────────────────────────────────────────────────

static ServerContext g_server_context;

ServerContext& server_ctx() { return g_server_context; }

} // namespace peadb
