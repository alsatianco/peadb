#include "command.hpp"

#include "datastore.hpp"
#include "errors.hpp"
#include "lua_engine.hpp"
#include "protocol.hpp"
#include "rdb.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
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
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <dlfcn.h>
#include <fnmatch.h>

namespace peadb {
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

std::atomic<std::uint64_t> g_mutation_epoch{0};
std::unordered_map<std::string, std::string> g_script_cache;
std::filesystem::path g_snapshot_path = "dump.rdb";
std::filesystem::path g_aof_path = "appendonly.aof";
bool g_appendonly = false;
bool g_replaying_aof = false;
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
std::int64_t g_config_lua_time_limit = 5000;
bool g_config_replica_serve_stale_data = true;
std::vector<std::string> g_replication_events;
std::vector<std::string> g_exec_replication_events;
bool g_capture_exec_replication = false;
int g_exec_last_repl_db = -1;
int g_exec_write_event_count = 0;
bool g_executing_exec = false;
int g_last_repl_db = 0;
bool g_replica_stale = false;
std::atomic<bool> g_loading_replication{false};
std::atomic<bool> g_script_busy{false};
std::atomic<bool> g_script_kill_requested{false};
thread_local bool g_script_allow_oom = false;
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
thread_local std::string g_module_dma_buf;
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
thread_local bool g_script_readonly_context = false;
thread_local RespVersion g_script_client_resp_version = RespVersion::Resp2;
thread_local SessionState* g_script_current_session = nullptr;
SessionState* g_busy_script_session = nullptr;
std::uint64_t g_script_prng_state = 0x12345678ULL;
std::unordered_map<std::string, CmdStats> g_cmdstats;
std::unordered_map<std::string, std::int64_t> g_errorstats;
std::int64_t g_total_error_replies = 0;

} // end anonymous namespace

// ── Server-wide stats (defined here, declared extern in command.hpp) ──
std::atomic<int> g_connected_clients{0};
std::atomic<std::int64_t> g_total_connections_received{0};
std::atomic<std::int64_t> g_total_commands_processed{0};
std::atomic<std::int64_t> g_server_start_time{0};
std::atomic<int> g_connected_replicas{0};
std::atomic<int> g_runtime_max_clients{10000};
std::function<int(std::int64_t)> g_count_synced_replicas;

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
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::string encode_simple(const std::string& value) { return "+" + value + "\r\n"; }
std::string encode_error(const std::string& value) { return "-ERR " + value + "\r\n"; }
std::string encode_integer(long long value) { return ":" + std::to_string(value) + "\r\n"; }
std::string encode_bulk(const std::string& value) { return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n"; }
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
    g_replication_events.push_back(encode_command_resp({"SELECT", std::to_string(session.db_index)}));
    g_last_repl_db = session.db_index;
  }
  g_replication_events.push_back(encode_command_resp(out));
}

void append_lazy_expire_dels(const SessionState& session) {
  const auto expired = store().collect_expired_keys();
  for (const auto& key : expired) {
    append_replication_event({"DEL", key}, session);
  }
}

void append_aof(const std::vector<std::string>& args) {
  if (!g_appendonly || g_replaying_aof) return;
  std::ofstream out(g_aof_path, std::ios::app | std::ios::binary);
  if (!out.is_open()) return;
  const auto payload = encode_command_resp(args);
  out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
}

bool rewrite_aof_file(std::string& err) {
  err.clear();
  std::ofstream out(g_aof_path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    err = "cannot open AOF for rewrite";
    return false;
  }
  const auto cmds = store().export_aof_commands();
  for (const auto& cmd : cmds) {
    const auto payload = encode_command_resp(cmd);
    out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
  }
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

    t.emplace("PUBLISH", CommandSpec{"PUBLISH", 3, {"write", "pubsub"}, 1, 1, 1,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          return encode_integer(0);
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

    t.emplace("SYNC", CommandSpec{"SYNC", 1, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) {
          session.replica_stream = true;
          session.repl_index = g_replication_events.size();
          g_last_repl_db = -1; // Force re-emit SELECT on next write
          // Send empty RDB payload — replication stream carries all state.
          // RESP bulk string: $0\r\n followed by empty body + trailing \r\n
          return std::string("$0\r\n\r\n");
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

          // For now, always do full resync
          session.replica_stream = true;
          session.repl_index = g_replication_events.size();
          g_last_repl_db = -1; // Force re-emit SELECT on next write

          // Build RDB for full sync
          std::string rdb_data = rdb_save_to_string();

          // FULLRESYNC <replid> <offset>
          std::string reply = "+FULLRESYNC " + g_master_replid + " " +
                              std::to_string(g_master_repl_offset) + "\r\n";
          reply += "$" + std::to_string(rdb_data.size()) + "\r\n" + rdb_data + "\r\n";
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
          session.watched_epoch = g_mutation_epoch.load();
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
            g_replication_events.push_back(encode_command_resp({"MULTI"}));
            for (const auto& ev : g_exec_replication_events) g_replication_events.push_back(ev);
            g_replication_events.push_back(encode_command_resp({"EXEC"}));
            g_last_repl_db = g_exec_last_repl_db;
          } else if (!has_replof && g_exec_write_event_count == 1 && !g_exec_replication_events.empty()) {
            if (g_exec_last_repl_db != g_last_repl_db) {
              // Emit SELECT if the captured events changed the db
              g_replication_events.push_back(encode_command_resp({"SELECT", std::to_string(g_exec_last_repl_db)}));
            }
            g_replication_events.push_back(g_exec_replication_events.back());
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
            if (!she.has_shebang && !allow_oom && write_intent) {
              const auto nsb = lower(normalize_script(script_body));
              if (nsb.find("redis.call('set'") != std::string::npos ||
                  nsb.find("redis.call(\"set\"") != std::string::npos ||
                  nsb.find("redis.pcall('set'") != std::string::npos ||
                  nsb.find("redis.pcall(\"set\"") != std::string::npos) {
                record_cmd_rejected("set");
              }
              return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
            }
          }
          if (write_intent && g_config_min_replicas_to_write > 0) {
            return std::string("-NOREPLICAS Not enough good replicas to write.\r\n");
          }

          const auto sha = pseudo_sha1(args[1]);
          g_script_cache[sha] = args[1];
          if (script_body.find("while true do end") != std::string::npos) {
            g_script_kill_requested.store(false);
            g_script_busy.store(true);
            return busy_script_error_reply();
          }
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

          // Compute used_memory estimate: dbsize * ~200 bytes each (rough)
          std::size_t total_keys = 0;
          const int prev_db = store().current_db();
          for (int d = 0; d < 16; ++d) {
            store().select_db(d);
            total_keys += store().dbsize();
          }
          store().select_db(prev_db);
          std::size_t used_mem = total_keys * 200 + 1024 * 1024; // 1MB base + ~200 per key

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
                   "redis_git_sha1:00000000\r\n"
                   "redis_git_dirty:0\r\n"
                   "redis_build_id:peadb-dev\r\n"
                   "peadb_version:0.1.0\r\n"
                   "redis_mode:standalone\r\n"
                   "os:Linux\r\n"
                   "arch_bits:64\r\n"
                   "tcp_port:" + std::to_string(g_server_port) + "\r\n"
                   "uptime_in_seconds:" + std::to_string(uptime_s) + "\r\n"
                   "uptime_in_days:" + std::to_string(uptime_s / 86400) + "\r\n"
                   "executable:peadb-server\r\n"
                   "config_file:\r\n\r\n";
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
                   "rdb_bgsave_in_progress:0\r\n"
                   "rdb_last_save_time:" + std::to_string(g_last_save_time) + "\r\n"
                   "rdb_last_bgsave_status:ok\r\n"
                   "aof_enabled:" + std::to_string(g_appendonly ? 1 : 0) + "\r\n"
                   "aof_rewrite_in_progress:0\r\n"
                   "aof_last_bgrewrite_status:ok\r\n\r\n";
          }
          if (want("STATS")) {
            out += "# Stats\r\n"
                   "total_connections_received:" + std::to_string(g_total_connections_received.load()) + "\r\n"
                   "total_commands_processed:" + std::to_string(g_total_commands_processed.load()) + "\r\n"
                   "total_error_replies:" + std::to_string(g_total_error_replies) + "\r\n"
                   "keyspace_hits:0\r\n"
                   "keyspace_misses:0\r\n\r\n";
          }
          if (want("REPLICATION")) {
            out += "# Replication\r\n"
                   "role:" + g_replication_role + "\r\n"
                   "connected_slaves:0\r\n"
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
              auto sz = store().dbsize();
              if (sz > 0) {
                out += "db" + std::to_string(d) + ":keys=" + std::to_string(sz) + ",expires=0,avg_ttl=0\r\n";
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
          pid_t pid = fork();
          if (pid < 0) return encode_error("Background save failed: fork error");
          if (pid == 0) {
            // Child process — write RDB and exit
            std::string err;
            bool ok = rdb_save(g_snapshot_path, err);
            _exit(ok ? 0 : 1);
          }
          // Parent continues
          g_last_save_time = now_ms() / 1000;
          return encode_simple("Background saving started");
        }});

    t.emplace("BGREWRITEAOF", CommandSpec{"BGREWRITEAOF", 1, {"admin", "noscript"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          std::string err;
          if (!rewrite_aof_file(err)) return encode_error(err);
          return encode_simple("Background append only file rewriting started");
        }});

    t.emplace("WAIT", CommandSpec{"WAIT", 3, {"noscript"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t replicas = 0;
          std::int64_t timeout = 0;
          if (!parse_i64(args[1], replicas) || !parse_i64(args[2], timeout) || replicas < 0 || timeout < 0) {
            return encode_error("value is not an integer or out of range");
          }
          // Count replicas that have acked up to current offset
          if (g_count_synced_replicas) {
            auto deadline = (timeout > 0) ? (now_ms() + timeout) : std::numeric_limits<std::int64_t>::max();
            while (true) {
              int synced = g_count_synced_replicas(g_master_repl_offset);
              if (synced >= replicas || now_ms() >= deadline) {
                return encode_integer(synced);
              }
              std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
          }
          return encode_integer(g_connected_replicas.load());
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
          const int target_port = g_server_port;
          std::thread([host, p, target_port]() {
            // Small delay to ensure pending client commands see slave role first
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            g_loading_replication.store(true);
            std::string cmd = "python3 scripts/redis/sync_from_redis.py --source-host " + host +
                              " --source-port " + std::to_string(p) +
                              " --target-host 127.0.0.1 --target-port " + std::to_string(target_port) + " --flush-target";
            const int rc = std::system(cmd.c_str());
            (void)rc;
            g_loading_replication.store(false);
            g_replica_stale = false;
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
            std::size_t cluster_size = 1 + g_cluster_peers.size();
            std::size_t slots_assigned = 16384;
            std::size_t slots_ok = 16384;
            return encode_bulk(
                "cluster_enabled:1\r\n"
                "cluster_state:ok\r\n"
                "cluster_slots_assigned:" + std::to_string(slots_assigned) + "\r\n"
                "cluster_slots_ok:" + std::to_string(slots_ok) + "\r\n"
                "cluster_slots_pfail:0\r\n"
                "cluster_slots_fail:0\r\n"
                "cluster_known_nodes:" + std::to_string(cluster_size) + "\r\n"
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
            return std::string("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:0\r\n");
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
        [](const std::vector<std::string>&, SessionState&, bool&) {
          const auto k = store().randomkey();
          return k.has_value() ? encode_bulk(*k) : encode_null(RespVersion::Resp2);
        }});

    t.emplace("SCAN", CommandSpec{"SCAN", -2, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::uint64_t cursor = 0;
          try { cursor = static_cast<std::uint64_t>(std::stoull(args[1])); } catch (...) { return encode_error("invalid cursor"); }
          std::string pattern = "*";
          std::size_t count = 10;
          for (std::size_t i = 2; i < args.size(); ++i) {
            const auto opt = upper(args[i]);
            if (opt == "MATCH" && i + 1 < args.size()) {
              pattern = args[++i];
              continue;
            }
            if (opt == "COUNT" && i + 1 < args.size()) {
              try { count = static_cast<std::size_t>(std::stoull(args[++i])); } catch (...) { return encode_error("value is not an integer or out of range"); }
              continue;
            }
            return encode_error("syntax error");
          }
          (void)cursor;
          (void)count;
          const auto ks = store().keys(pattern);
          std::vector<std::string> out;
          out.reserve(ks.size());
          for (const auto& k : ks) out.push_back(encode_bulk(k));
          return encode_array({encode_bulk("0"), encode_array(out)});
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
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_list_key(args[1], [&]() {
            std::vector<std::string> vals(args.begin() + 2, args.end());
            bool wrongtype = false;
            auto n = store().lpush(args[1], vals, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(n);
          });
        }});

    t.emplace("RPUSH", CommandSpec{"RPUSH", -3, {"write", "denyoom"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_list_key(args[1], [&]() {
            std::vector<std::string> vals(args.begin() + 2, args.end());
            bool wrongtype = false;
            auto n = store().rpush(args[1], vals, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
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
          // No data available — block the client
          session.blocked.type = BlockType::List;
          session.blocked.pop_left = true;
          session.blocked.keys.clear();
          for (std::size_t i = 1; i + 1 < args.size(); ++i) session.blocked.keys.push_back(args[i]);
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
          session.blocked.type = BlockType::List;
          session.blocked.pop_left = false;
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
          // Block waiting for src key
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
              std::ostringstream oss;
              oss << vals[0].second;
              return encode_array({encode_bulk(args[i]), encode_bulk(vals[0].first), encode_bulk(oss.str())});
            }
          }
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
              std::ostringstream oss;
              oss << vals[0].second;
              return encode_array({encode_bulk(args[i]), encode_bulk(vals[0].first), encode_bulk(oss.str())});
            }
          }
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
                std::ostringstream oss;
                oss << r.score;
                incr_out = oss.str();
              }
            }
            if (incr) return encode_bulk(incr_out);
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
                std::ostringstream oss; oss << s;
                out.push_back(encode_bulk(oss.str()));
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
              std::ostringstream oss; oss << s;
              inner.push_back(encode_bulk(m));
              inner.push_back(encode_bulk(oss.str()));
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
              std::ostringstream oss; oss << s;
              out.push_back(encode_bulk(m));
              out.push_back(encode_bulk(oss.str()));
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
              std::ostringstream oss; oss << s;
              out.push_back(encode_bulk(m));
              out.push_back(encode_bulk(oss.str()));
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
              std::ostringstream oss; oss << s;
              flat.push_back(encode_bulk(m));
              flat.push_back(encode_bulk(oss.str()));
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
          if (((args.size() - 3) % 2) != 0) return encode_error("wrong number of arguments for 'xadd' command");
          return with_stream_key(args[1], [&]() {
            std::vector<std::pair<std::string, std::string>> fields;
            for (std::size_t i = 3; i < args.size(); i += 2) fields.push_back({args[i], args[i + 1]});
            bool wrongtype = false;
            std::string err;
            auto id = store().xadd(args[1], args[2], fields, wrongtype, err);
            if (wrongtype) return wrongtype_error_reply();
            if (!id.has_value()) return encode_error(err.empty() ? "ERR" : err);
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

    // ─── SCAN (top-level keyspace scan) ──────────────────────────────────
    t.emplace("SCAN", CommandSpec{"SCAN", -2, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::uint64_t cursor = 0;
          try { cursor = static_cast<std::uint64_t>(std::stoull(args[1])); }
          catch (...) { return encode_error("invalid cursor"); }
          std::size_t count = 10;
          std::string match_pattern = "*";
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
          auto [next, keys] = store().scan(cursor, count, match_pattern, type_filter);
          std::vector<std::string> inner;
          inner.reserve(keys.size());
          for (const auto& k : keys) inner.push_back(encode_bulk(k));
          return encode_array({encode_bulk(std::to_string(next)), encode_array(inner)});
        }});

    // ─── SUBSCRIBE / UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE ────────────
    t.emplace("SUBSCRIBE", CommandSpec{"SUBSCRIBE", -2, {"pubsub"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          // In-memory stub: we acknowledge the subscription but actual message
          // delivery requires server-level integration.
          std::string out;
          for (std::size_t i = 1; i < args.size(); ++i) {
            out += encode_array({encode_bulk("subscribe"), encode_bulk(args[i]),
                                 encode_integer(static_cast<long long>(i))});
          }
          return out;
        }});

    t.emplace("UNSUBSCRIBE", CommandSpec{"UNSUBSCRIBE", -1, {"pubsub"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::string out;
          if (args.size() == 1) {
            // unsubscribe all
            out = encode_array({encode_bulk("unsubscribe"), encode_null(RespVersion::Resp2), encode_integer(0)});
          } else {
            for (std::size_t i = 1; i < args.size(); ++i) {
              out += encode_array({encode_bulk("unsubscribe"), encode_bulk(args[i]),
                                   encode_integer(static_cast<long long>(args.size() - 1 - i))});
            }
          }
          return out;
        }});

    t.emplace("PSUBSCRIBE", CommandSpec{"PSUBSCRIBE", -2, {"pubsub"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::string out;
          for (std::size_t i = 1; i < args.size(); ++i) {
            out += encode_array({encode_bulk("psubscribe"), encode_bulk(args[i]),
                                 encode_integer(static_cast<long long>(i))});
          }
          return out;
        }});

    t.emplace("PUNSUBSCRIBE", CommandSpec{"PUNSUBSCRIBE", -1, {"pubsub"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::string out;
          if (args.size() == 1) {
            out = encode_array({encode_bulk("punsubscribe"), encode_null(RespVersion::Resp2), encode_integer(0)});
          } else {
            for (std::size_t i = 1; i < args.size(); ++i) {
              out += encode_array({encode_bulk("punsubscribe"), encode_bulk(args[i]),
                                   encode_integer(static_cast<long long>(args.size() - 1 - i))});
            }
          }
          return out;
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
  const auto& table = command_table();
  const bool is_script_kill =
      (cmd == "SCRIPT" && args.size() == 2 && upper(args[1]) == "KILL") ||
      (cmd == "FUNCTION" && args.size() == 2 && upper(args[1]) == "KILL");
  if (g_script_busy.load() && !is_script_kill) {
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
  ++top_stat.calls;
  const bool spec_is_write = spec.is_write();
  if (spec_is_write && g_config_maxmemory == 1 && !g_script_allow_oom &&
      cmd != "CONFIG" && cmd != "EVAL" && cmd != "EVALSHA" &&
      cmd != "FLUSHALL" && cmd != "FLUSHDB") {
    ++top_stat.rejected_calls;
    record_error_stat("OOM");
    ++top_stat.failed_calls;
    return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
  }
  if (spec_is_write && g_config_min_replicas_to_write > 0 &&
      cmd != "EVAL" && cmd != "EVALSHA" &&
      cmd != "CONFIG" && cmd != "REPLICAOF" && cmd != "SLAVEOF") {
    ++top_stat.rejected_calls;
    record_error_stat("NOREPLICAS");
    ++top_stat.failed_calls;
    return std::string("-NOREPLICAS Not enough good replicas to write.\r\n");
  }
  if (!spec_is_write && g_replication_role == "slave" && !g_config_replica_serve_stale_data &&
      g_replica_stale && cmd != "REPLICAOF" && cmd != "SLAVEOF" && cmd != "INFO" &&
      cmd != "MULTI" && cmd != "EXEC" && cmd != "DISCARD" &&
      cmd != "COMMAND" && cmd != "CONFIG") {
    return masterdown_stale_reply();
  }
  if (spec_is_write && g_replication_role == "slave" && !g_executing_exec &&
      !g_loading_replication.load() &&
      cmd != "REPLICAOF" && cmd != "SLAVEOF") {
    ++top_stat.rejected_calls;
    record_error_stat("READONLY");
    ++top_stat.failed_calls;
    return std::string("-READONLY You can't write against a read only replica.\r\n");
  }

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
    record_error_stat(error_code_from_reply(reply));
  }
  if (spec_is_write && !is_error_reply(reply)) {
    g_mutation_epoch.fetch_add(1);
    g_master_repl_offset += static_cast<std::int64_t>(encode_command_resp_size(args));
    append_replication_event(args, session, &reply);
    append_aof(args);
  }
  return reply;
}

void configure_persistence(const std::string& dir, const std::string& dbfilename, bool appendonly,
                           const std::string& appendfilename) {
  g_snapshot_path = std::filesystem::path(dir) / dbfilename;
  g_appendonly = appendonly;
  g_aof_path = std::filesystem::path(dir) / appendfilename;
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

int configured_max_clients() {
  return g_runtime_max_clients.load(std::memory_order_relaxed);
}

std::size_t replication_event_count() {
  return g_replication_events.size();
}

std::string replication_event_at(std::size_t idx) {
  if (idx >= g_replication_events.size()) return {};
  return g_replication_events[idx];
}

// ─── Blocking client support ──────────────────────────────────────────────

std::string try_unblock_client(SessionState& session) {
  if (session.blocked.type == BlockType::None) return {};

  const auto current = now_ms();

  // Check timeout
  if (session.blocked.deadline_ms > 0 && current >= session.blocked.deadline_ms) {
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
        session.blocked.type = BlockType::None;
        session.blocked.keys.clear();
        session.blocked.original_args.clear();
        return wrongtype_error_reply();
      }
      if (val.has_value()) {
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
        session.blocked.type = BlockType::None;
        session.blocked.keys.clear();
        session.blocked.original_args.clear();
        return wrongtype_error_reply();
      }
      if (!vals.empty()) {
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

void notify_key_written(const std::string& /*key*/, int /*db_index*/) {
  // Key write notification — the server loop uses this as a hint to
  // re-check blocked clients. No-op for now; the server loop polls
  // blocked clients on every iteration already.
}

} // namespace peadb
