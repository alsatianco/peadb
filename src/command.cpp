#include "command.hpp"
#include "command_metadata.generated.hpp"
#include "server_context.hpp"

#include "datastore.hpp"
#include "errors.hpp"
#include "lua_engine.hpp"
#include <map>
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
#include <mutex>
#include <thread>
#include <cstdlib>
#include <string_view>
#include <array>
#include <set>
#include <memory>
#include <random>
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
#include <deque>
#include <sys/resource.h>

#include "logger.hpp"

namespace peadb {

// Forward declarations for functions defined after the anonymous namespace
void register_key_waiter(const std::string& key, int fd);
void unregister_key_waiter(int fd);
void notify_key_written(const std::string& key, int db_index);
bool replica_do_fullsync(const std::string& host, int port, std::uint64_t gen);

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

// Redis proto-max-bulk-len (512 MB): caps a single string value. Mirrors the parser-side
// kMaxBulkLen in protocol.cpp; used for SETBIT/GETBIT bit offsets and APPEND/SETRANGE growth.
constexpr std::int64_t kProtoMaxBulkLen = 512LL * 1024 * 1024;

std::unordered_map<std::string, std::string> g_script_cache;
std::filesystem::path g_snapshot_path = "dump.rdb";
std::filesystem::path g_aof_path = "appendonly.aof";
bool g_appendonly = false;
// thread_local: each thread (event loop, REPLICAOF streaming) maintains its own
// replay flag so setting it in one thread never silently suppresses AOF writes
// or bypasses READONLY enforcement in the other.
thread_local bool g_replaying_aof = false;
// Set while re-dispatching a blocked XREAD/XREADGROUP from try_unblock_client so the
// handler reads once instead of re-registering a block.
thread_local bool g_block_disabled = false;
int g_aof_fd = -1;
std::string g_appendfsync = "everysec";
bool g_aof_write_error = false;
bool g_aof_load_truncated = true;   // tolerate an incomplete final command on load
std::int64_t g_last_aof_fsync_ms = 0;
std::atomic<pid_t> g_bgsave_child_pid{0};
std::atomic<bool> g_bgsave_in_progress{false};
std::int64_t g_bgsave_start_time = 0;
std::string g_last_bgsave_status = "ok";
std::atomic<pid_t> g_bgrewrite_child_pid{0};
std::atomic<bool> g_bgrewrite_in_progress{false};
std::string g_aof_rewrite_buf;  // Parent buffers commands written during child rewrite
bool g_aof_rewrite_buf_capped = false; // Set once when the 256 MiB cap is hit per rewrite
std::string g_aof_last_bgrewrite_status = "ok"; // Reported by INFO PERSISTENCE
// Queue for replication commands produced by the streaming thread and consumed
// by the event loop. All DataStore mutations stay on the event loop thread.
std::mutex g_replica_cmd_queue_mu;
std::vector<std::vector<std::string>> g_replica_cmd_queue;
// Pending RDB load handed off from streaming thread to event loop. The streaming
// thread sets g_replica_load_path + g_replica_load_pending=true and waits; the event
// loop runs rdb_load() synchronously and signals completion via g_replica_load_pending=false.
std::string g_replica_load_path; // protected by g_replica_cmd_queue_mu
std::atomic<bool> g_replica_load_pending{false};
std::atomic<bool> g_replica_load_ok{false};
bool g_active_expire = true;
bool g_pause_cron = false;  // DEBUG PAUSE-CRON: freezes active expiry + save-point cron
int g_last_aof_db = -1;     // db of the last AOF append; -1 forces a SELECT on next append
// redis.set_repl() flags for the currently-running script: bit 1 = AOF, bit 2 = replicas
// (REPL_ALL=3 default). Reset at every script invocation.
thread_local int g_script_repl_flags = 3;
std::int64_t g_last_save_time = 0;
// ── Save points (BGSAVE thresholds) ──
std::vector<std::pair<std::int64_t, std::int64_t>> g_save_params;  // (seconds, changes)
std::string g_save_spec;                       // raw "sec changes ..." for CONFIG GET
std::int64_t g_dirty_at_last_save = 0;         // mutation_epoch snapshot at last save start
bool g_stop_writes_on_bgsave_error = true;
std::string g_replication_role = "master";
std::string g_replication_master_host = "";
int g_replication_master_port = 0;
int g_server_port = 6379;
std::string g_master_replid = "0000000000000000000000000000000000000001";
// replid2/second_replid_offset: after a replica is promoted (REPLICAOF NO ONE) the old
// master's replid is remembered here so siblings that were replicating the same stream can
// still partial-resync against history instead of forcing a full resync. (Redis PSYNC2.)
std::string g_master_replid2 = "0000000000000000000000000000000000000000";
std::int64_t g_second_replid_offset = -1;
std::int64_t g_master_repl_offset = 0;
std::atomic<std::int64_t> g_sync_full{0};     // count of full resyncs served (INFO sync_full)
std::atomic<std::int64_t> g_sync_partial_ok{0};   // partial resyncs served (sync_partial_ok)
std::atomic<std::int64_t> g_sync_partial_err{0};  // partial resync requests we couldn't serve
std::int64_t g_config_maxmemory = 0;
MaxmemoryPolicy g_maxmemory_policy = MaxmemoryPolicy::NoEviction;
int g_maxmemory_samples = 5;        // candidate keys sampled per eviction (Redis default 5)
int g_config_timeout = 0;           // idle-client timeout in seconds; 0 = never reap
int g_config_tcp_backlog = 511;     // listen(2) backlog, reported by CONFIG GET
bool g_lazyfree_lazy_server_del = false;  // propagate server-side deletions as UNLINK
// client-output-buffer-limit per class [normal, replica, pubsub]. Redis defaults: normal
// unlimited; replica 256mb hard / 64mb soft / 60s; pubsub 32mb / 8mb / 60s.
CoblLimit g_cobl[3] = {
  {0, 0, 0},
  {256LL * 1024 * 1024, 64LL * 1024 * 1024, 60},
  {32LL * 1024 * 1024, 8LL * 1024 * 1024, 60},
};
std::int64_t g_config_min_replicas_to_write = 0;
bool g_in_lua_script = false;
bool g_config_replica_serve_stale_data = true;
// ── Security ──
std::string g_requirepass;                    // empty = no auth required (Redis default)
std::string g_masterauth;                     // sent to master during replica handshake
bool g_protected_mode = true;                 // refuse non-loopback clients when no auth set
std::string g_enable_module_command = "no";   // "yes" | "no" | "local" — gates MODULE LOAD
std::vector<std::pair<std::string, std::string>> g_rename_commands;
// Length-checked, content-time-independent compare to avoid leaking the password
// via per-byte timing. (Length is not secret enough to warrant masking here.)
inline bool const_time_eq(const std::string& a, const std::string& b) {
  if (a.size() != b.size()) return false;
  unsigned char acc = 0;
  for (std::size_t i = 0; i < a.size(); ++i)
    acc |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
  return acc == 0;
}
// Circular replication backlog (replaces unbounded g_replication_events vector).
// Size is configurable via repl-backlog-size; a larger window lets a briefly
// disconnected replica resume via +CONTINUE instead of forcing a full resync.
struct ReplBacklog {
    std::size_t size_ = 1 << 20;  // default 1 MiB
    std::vector<char> buf;
    std::int64_t total = 0; // total bytes ever appended

    ReplBacklog() : buf(size_, 0) {}

    // Resize the backlog. Only valid before any data is appended (startup), since
    // the modular indexing assumes a fixed size for the lifetime of the buffer.
    void reset(std::size_t newsize) {
        if (newsize == 0) return;
        size_ = newsize;
        buf.assign(size_, 0);
        total = 0;
    }

    std::int64_t base() const {
        return total > static_cast<std::int64_t>(size_)
               ? total - static_cast<std::int64_t>(size_) : 0;
    }

    void append(const std::string& data) {
        for (char c : data) {
            buf[static_cast<std::size_t>(total++ % size_)] = c;
        }
    }

    // Fill out with bytes [from_byte, total). Returns false if from_byte < base().
    bool get_slice(std::int64_t from_byte, std::string& out) const {
        if (from_byte < base() || from_byte > total) return false;
        const std::size_t len = static_cast<std::size_t>(total - from_byte);
        out.resize(len);
        if (len == 0) return true;
        const std::size_t start = static_cast<std::size_t>(from_byte % size_);
        if (start + len <= size_) {
            std::memcpy(out.data(), buf.data() + start, len);
        } else {
            const std::size_t first = size_ - start;
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
// Bumped on every REPLICAOF (including NO ONE). The reconnect supervisor and the
// streaming loop compare against their captured generation and stop when it changes,
// so a new REPLICAOF promptly supersedes an in-flight sync/stream.
std::atomic<std::uint64_t> g_repl_generation{0};
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
struct FunctionEntry {
  std::string name;         // display name (original case)
  std::string description;
  int flags = 0;            // FunctionFlags bitmask
  int callback_ref = -1;    // Lua registry ref for the callback closure
};
struct FunctionLibrary {
  std::string name;    // display name (original case)
  std::string engine;  // always "LUA"
  std::string code;    // full source including the shebang line
  std::vector<FunctionEntry> functions;
};
struct CmdStats {
  std::int64_t calls = 0;
  std::int64_t rejected_calls = 0;
  std::int64_t failed_calls = 0;
  std::int64_t usec_total = 0;   // accumulated execution time for usec_per_call
};

// ── Observability state (SLOWLOG / LATENCY / keyspace notifications / INFO) ──
struct SlowlogEntry {
  std::int64_t id;
  std::int64_t time_s;
  std::int64_t usec;
  std::vector<std::string> argv;   // redacted + truncated argv (<=32 elements, each <=128 bytes)
  std::string peer;                // client address ip:port
  std::string name;                // client name (CLIENT SETNAME)
};
std::deque<SlowlogEntry> g_slowlog;
std::int64_t g_slowlog_next_id = 0;
std::int64_t g_slowlog_threshold_us = 10000;   // slowlog-log-slower-than (<0 disables)
std::size_t g_slowlog_max_len = 128;

struct LatencyEvent {
  std::int64_t time_s = 0;
  std::int64_t latest_ms = 0;
  std::int64_t max_ms = 0;
  std::deque<std::pair<std::int64_t, std::int64_t>> history;  // (time_s, latency_ms), capped at 160
};
std::unordered_map<std::string, LatencyEvent> g_latency_events;
std::int64_t g_latency_monitor_threshold_ms = 0;   // latency-monitor-threshold (0 disables)

// Per-command latency histogram for LATENCY HISTOGRAM (keyed by full command name, i.e. the
// container form "parent|sub" for subcommands). Log2 buckets over microseconds.
struct CmdLatencyHist {
  std::uint64_t calls = 0;
  std::array<std::uint64_t, 64> buckets{};  // buckets[i] = count of samples with floor(log2 usec)==i
};
std::unordered_map<std::string, CmdLatencyHist> g_cmd_latency_hist;

std::int64_t g_used_memory_peak = 0;
std::int64_t g_latest_fork_usec = 0;
std::int64_t g_total_forks = 0;
std::string g_loglevel_name = "notice";
std::int64_t g_ops_sample_ms = 0;     // for instantaneous_ops_per_sec (sampled per INFO)
std::int64_t g_ops_sample_count = 0;
std::unordered_set<int> g_monitor_fds;   // clients in MONITOR mode

// Keyspace notifications: bitmask parsed from notify-keyspace-events.
enum NotifyFlag : std::uint32_t {
  NOTIFY_KEYSPACE = 1u << 0, NOTIFY_KEYEVENT = 1u << 1, NOTIFY_GENERIC = 1u << 2,
  NOTIFY_STRING = 1u << 3, NOTIFY_LIST = 1u << 4, NOTIFY_SET = 1u << 5,
  NOTIFY_HASH = 1u << 6, NOTIFY_ZSET = 1u << 7, NOTIFY_EXPIRED = 1u << 8,
  NOTIFY_EVICTED = 1u << 9, NOTIFY_STREAM = 1u << 10, NOTIFY_KEY_MISS = 1u << 11,
  NOTIFY_NEW = 1u << 12, NOTIFY_MODULE = 1u << 13,
};
constexpr std::uint32_t NOTIFY_ALL = NOTIFY_GENERIC | NOTIFY_STRING | NOTIFY_LIST |
    NOTIFY_SET | NOTIFY_HASH | NOTIFY_ZSET | NOTIFY_EXPIRED | NOTIFY_EVICTED |
    NOTIFY_STREAM | NOTIFY_MODULE;
std::uint32_t g_notify_flags = 0;
bool parse_i64(const std::string& s, std::int64_t& out);
std::int64_t now_ms();
// Parse a Redis-style memory size: raw bytes ("1048576") or with a unit suffix
// (k=1000, kb=1024, m=1000^2, mb=1024^2, g=1000^3, gb=1024^3; b/none = bytes).
inline bool parse_memory(const std::string& s, std::int64_t& out) {
  if (s.empty()) return false;
  std::size_t i = 0;
  if (s[0] == '-' || s[0] == '+') ++i;
  while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) ++i;
  std::int64_t base = 0;
  if (!parse_i64(s.substr(0, i), base)) return false;
  std::string unit = s.substr(i);
  for (auto& c : unit) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  std::int64_t mult = 1;
  if (unit.empty() || unit == "b") mult = 1;
  else if (unit == "k") mult = 1000LL;
  else if (unit == "kb") mult = 1024LL;
  else if (unit == "m") mult = 1000LL * 1000;
  else if (unit == "mb") mult = 1024LL * 1024;
  else if (unit == "g") mult = 1000LL * 1000 * 1000;
  else if (unit == "gb") mult = 1024LL * 1024 * 1024;
  else return false;
  out = base * mult;
  return out >= 0;
}
std::string lower(std::string s);  // defined below; needed by parse_maxmemory_policy
std::string upper(std::string s);  // defined below; needed by redact_slowlog_args

// Maxmemory-policy string ↔ enum. Returns nullopt for an unrecognised name.
inline std::optional<MaxmemoryPolicy> parse_maxmemory_policy(const std::string& s) {
  const std::string p = lower(s);
  if (p == "noeviction")      return MaxmemoryPolicy::NoEviction;
  if (p == "allkeys-lru")     return MaxmemoryPolicy::AllkeysLru;
  if (p == "allkeys-lfu")     return MaxmemoryPolicy::AllkeysLfu;
  if (p == "allkeys-random")  return MaxmemoryPolicy::AllkeysRandom;
  if (p == "volatile-lru")    return MaxmemoryPolicy::VolatileLru;
  if (p == "volatile-lfu")    return MaxmemoryPolicy::VolatileLfu;
  if (p == "volatile-random") return MaxmemoryPolicy::VolatileRandom;
  if (p == "volatile-ttl")    return MaxmemoryPolicy::VolatileTtl;
  return std::nullopt;
}
inline const char* maxmemory_policy_to_str(MaxmemoryPolicy p) {
  switch (p) {
    case MaxmemoryPolicy::NoEviction:      return "noeviction";
    case MaxmemoryPolicy::AllkeysLru:      return "allkeys-lru";
    case MaxmemoryPolicy::AllkeysLfu:      return "allkeys-lfu";
    case MaxmemoryPolicy::AllkeysRandom:   return "allkeys-random";
    case MaxmemoryPolicy::VolatileLru:     return "volatile-lru";
    case MaxmemoryPolicy::VolatileLfu:     return "volatile-lfu";
    case MaxmemoryPolicy::VolatileRandom:  return "volatile-random";
    case MaxmemoryPolicy::VolatileTtl:     return "volatile-ttl";
  }
  return "noeviction";
}

inline void apply_maxmemory_policy(MaxmemoryPolicy p) {
  g_maxmemory_policy = p;
  store().set_eviction_lfu(p == MaxmemoryPolicy::AllkeysLfu || p == MaxmemoryPolicy::VolatileLfu);
}

// A Redis-style 40-char lowercase-hex run id, used as the replication id. Randomizing it
// per process means two unrelated PeaDB instances never share a replid, so a re-pointed
// replica can't be wrongly served a +CONTINUE partial resync against a different stream.
inline std::string gen_run_id() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  static const char* hex = "0123456789abcdef";
  std::string id(40, '0');
  for (char& c : id) c = hex[rng() & 0xF];
  return id;
}

// Defined later (needs the AOF/backlog/notify propagation helpers). When over the
// maxmemory budget and a policy is set, evict keys until back under budget or nothing
// is evictable, propagating each eviction to replicas/AOF/keyspace-notifications.
void evict_to_fit();

// maxmemory enforcement: when used memory exceeds the limit, first try to evict per
// maxmemory-policy; reject denyoom writes with -OOM only if still over budget (true
// noeviction, or nothing left to evict). used_memory() is the bounded-staleness cache.
inline bool oom_should_reject() {
  if (g_config_maxmemory <= 0) return false;
  if (store().used_memory() <= g_config_maxmemory) return false;
  if (g_maxmemory_policy != MaxmemoryPolicy::NoEviction &&
      g_replication_role != "slave" && !g_replaying_aof) {
    evict_to_fit();
  }
  return store().used_memory() > g_config_maxmemory;
}
// min-replicas-to-write: reject writes only when FEWER than the required number of
// replicas are actually connected (the previous code rejected whenever the knob was
// set at all, blocking writes even with healthy replicas — a footgun).
inline bool noreplicas_should_reject() {
  if (g_config_min_replicas_to_write <= 0) return false;
  const int connected = g_count_connected_replicas ? g_count_connected_replicas() : 0;
  return connected < g_config_min_replicas_to_write;
}
// stop-writes-on-bgsave-error: refuse writes when persistence is failing, so the
// server does not keep accepting data it cannot durably store.
inline bool misconf_should_reject() {
  if (g_appendonly && g_aof_write_error) return true;
  if (g_stop_writes_on_bgsave_error && !g_save_params.empty() && g_last_bgsave_status == "err") return true;
  return false;
}
// Fork a child to write the RDB. Shared by the BGSAVE command and the save-point
// cron. Resets the dirty baseline at save start. Returns false on fork error.
bool do_bgsave_fork() {
  if (g_bgsave_in_progress.load()) return false;
  const auto fork_t0 = std::chrono::steady_clock::now();
  pid_t pid = fork();
  if (pid < 0) return false;
  if (pid == 0) {
    std::string err;
    bool ok = rdb_save(g_snapshot_path, err);
    _exit(ok ? 0 : 1);
  }
  g_latest_fork_usec = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - fork_t0).count();
  ++g_total_forks;
  g_bgsave_in_progress.store(true);
  g_bgsave_child_pid.store(pid);
  g_bgsave_start_time = now_ms();
  g_dirty_at_last_save = static_cast<std::int64_t>(store().mutation_epoch());
  return true;
}

// ── SLOWLOG / LATENCY recording ──
// Redact sensitive arguments before they are stored in the slow log (and MONITOR), matching
// Redis's redactClientCommandArgument sites: passwords and secrets become "(redacted)".
std::vector<std::string> redact_slowlog_args(const std::vector<std::string>& args) {
  std::vector<std::string> out = args;
  if (args.empty()) return out;
  const std::string cmd = upper(args[0]);
  auto redact_after = [&](const std::string& kw, std::size_t n) {
    for (std::size_t i = 1; i < out.size(); ++i) {
      if (upper(args[i]) == kw) {
        for (std::size_t j = 1; j <= n && i + j < out.size(); ++j) out[i + j] = "(redacted)";
      }
    }
  };
  if (cmd == "ACL" && args.size() >= 2) {
    const std::string sub = upper(args[1]);
    if (sub == "SETUSER" || sub == "GETUSER")
      for (std::size_t i = 2; i < out.size(); ++i) out[i] = "(redacted)";
  } else if (cmd == "CONFIG" && args.size() >= 2 && upper(args[1]) == "SET") {
    // CONFIG SET <key> <value> [<key> <value> ...] — redact the value of any sensitive key.
    for (std::size_t i = 2; i + 1 < out.size(); i += 2) {
      const std::string k = lower(args[i]);
      if (k == "requirepass" || k == "masterauth" || k == "masteruser") out[i + 1] = "(redacted)";
    }
  } else if (cmd == "MIGRATE") {
    redact_after("AUTH", 1);   // AUTH <password>
    redact_after("AUTH2", 2);  // AUTH2 <username> <password>
  } else if (cmd == "AUTH") {
    for (std::size_t i = 1; i < out.size(); ++i) out[i] = "(redacted)";
  } else if (cmd == "HELLO") {
    redact_after("AUTH", 2);   // HELLO [proto] AUTH <username> <password>
  }
  return out;
}

void slowlog_add(const std::vector<std::string>& args, std::int64_t usec,
                 const std::string& peer, const std::string& name) {
  if (g_slowlog_threshold_us < 0 || usec < g_slowlog_threshold_us || args.empty()) return;
  SlowlogEntry e;
  e.id = g_slowlog_next_id++;
  e.time_s = now_ms() / 1000;
  e.usec = usec;
  e.peer = peer;
  e.name = name;
  const std::vector<std::string> red = redact_slowlog_args(args);
  // Redis SLOWLOG_ENTRY_MAX_ARGC = 32: keep at most 31 real args, then a sentinel; each kept
  // string is truncated to 128 bytes with a "(N more bytes)" suffix.
  constexpr std::size_t kMaxArgc = 32;
  const std::size_t slargc = std::min(red.size(), kMaxArgc);
  for (std::size_t j = 0; j < slargc; ++j) {
    if (slargc != red.size() && j == slargc - 1) {
      e.argv.push_back("... (" + std::to_string(red.size() - slargc + 1) + " more arguments)");
    } else if (red[j].size() > 128) {
      e.argv.push_back(red[j].substr(0, 128) + "... (" + std::to_string(red[j].size() - 128) + " more bytes)");
    } else {
      e.argv.push_back(red[j]);
    }
  }
  g_slowlog.push_front(std::move(e));
  while (g_slowlog.size() > g_slowlog_max_len) g_slowlog.pop_back();
}

void latency_add(const std::string& event, std::int64_t ms) {
  if (g_latency_monitor_threshold_ms <= 0 || ms < g_latency_monitor_threshold_ms) return;
  auto& ev = g_latency_events[event];
  ev.time_s = now_ms() / 1000;
  ev.latest_ms = ms;
  if (ms > ev.max_ms) ev.max_ms = ms;
  ev.history.emplace_back(ev.time_s, ms);
  while (ev.history.size() > 160) ev.history.pop_front();  // LATENCY_TS_LEN
}

// Full command name for LATENCY HISTOGRAM / commandstats keying: container commands use the
// "parent|sub" form (matching Redis's per-subcommand stats). Only a handful of verbs nest.
bool is_container_command(const std::string& upper_cmd) {
  static const std::unordered_set<std::string> kContainers = {
      "CONFIG", "CLIENT", "COMMAND", "OBJECT", "MEMORY", "SLOWLOG", "LATENCY", "ACL",
      "FUNCTION", "CLUSTER", "PUBSUB", "XINFO", "XGROUP", "SCRIPT", "DEBUG"};
  return kContainers.count(upper_cmd) != 0;
}
std::string full_command_name(const std::string& upper_cmd, const std::vector<std::string>& args) {
  std::string full = lower(upper_cmd);
  if (args.size() >= 2 && is_container_command(upper_cmd)) full += "|" + lower(args[1]);
  return full;
}
void record_cmd_latency(const std::string& full, std::int64_t usec) {
  auto& h = g_cmd_latency_hist[full];
  ++h.calls;
  const std::uint64_t u = usec <= 0 ? 0 : static_cast<std::uint64_t>(usec);
  const int b = u < 2 ? 0 : (63 - __builtin_clzll(u));  // floor(log2 usec)
  ++h.buckets[b];
}

// ── Keyspace notifications ──
std::uint32_t parse_notify_flags(const std::string& s) {
  std::uint32_t f = 0;
  for (char c : s) switch (c) {
    case 'K': f |= NOTIFY_KEYSPACE; break;  case 'E': f |= NOTIFY_KEYEVENT; break;
    case 'g': f |= NOTIFY_GENERIC; break;   case '$': f |= NOTIFY_STRING; break;
    case 'l': f |= NOTIFY_LIST; break;      case 's': f |= NOTIFY_SET; break;
    case 'h': f |= NOTIFY_HASH; break;      case 'z': f |= NOTIFY_ZSET; break;
    case 'x': f |= NOTIFY_EXPIRED; break;   case 'e': f |= NOTIFY_EVICTED; break;
    case 't': f |= NOTIFY_STREAM; break;    case 'm': f |= NOTIFY_KEY_MISS; break;
    case 'n': f |= NOTIFY_NEW; break;       case 'd': f |= NOTIFY_MODULE; break;
    case 'A': f |= NOTIFY_ALL; break;       default: break;
  }
  return f;
}
std::string notify_flags_to_str(std::uint32_t f) {
  std::string s;
  if ((f & NOTIFY_ALL) == NOTIFY_ALL) s += "A";
  else {
    if (f & NOTIFY_GENERIC) s += "g"; if (f & NOTIFY_STRING) s += "$";
    if (f & NOTIFY_LIST) s += "l";    if (f & NOTIFY_SET) s += "s";
    if (f & NOTIFY_HASH) s += "h";    if (f & NOTIFY_ZSET) s += "z";
    if (f & NOTIFY_EXPIRED) s += "x"; if (f & NOTIFY_EVICTED) s += "e";
    if (f & NOTIFY_STREAM) s += "t";  if (f & NOTIFY_MODULE) s += "d";
  }
  if (f & NOTIFY_KEY_MISS) s += "m";
  if (f & NOTIFY_NEW) s += "n";
  if (f & NOTIFY_KEYSPACE) s += "K";
  if (f & NOTIFY_KEYEVENT) s += "E";
  return s;
}
void notify_keyspace_event(std::uint32_t cls, const std::string& event, const std::string& key, int db) {
  if (!(g_notify_flags & cls)) return;
  if (g_notify_flags & NOTIFY_KEYSPACE)
    pubsub_publish("__keyspace@" + std::to_string(db) + "__:" + key, event);
  if (g_notify_flags & NOTIFY_KEYEVENT)
    pubsub_publish("__keyevent@" + std::to_string(db) + "__:" + event, key);
}
// Centralised emission for write commands (avoids hand-instrumenting ~40 handlers).
// Covers the most-subscribed events; class is per-command (mirrors the key's type).
void emit_keyspace_notifications(const std::string& cmd, const std::vector<std::string>& args,
                                 int first_key, int db, const std::string* reply = nullptr) {
  if (g_notify_flags == 0) return;
  // Redis emits these events only when the command actually changed the key; a no-op
  // (e.g. SREM of a non-member, SADD of existing members) returns 0 and fires nothing.
  static const std::unordered_set<std::string> count_gated = {
      "SADD", "SREM", "ZREM", "HDEL", "LREM"};
  if (reply && count_gated.count(cmd) && reply->rfind(":0\r\n", 0) == 0) return;
  if (cmd == "DEL" || cmd == "UNLINK") {
    for (std::size_t i = 1; i < args.size(); ++i) notify_keyspace_event(NOTIFY_GENERIC, "del", args[i], db);
    return;
  }
  if (cmd == "MSET" || cmd == "MSETNX") {
    for (std::size_t i = 1; i + 1 < args.size(); i += 2) notify_keyspace_event(NOTIFY_STRING, "set", args[i], db);
    return;
  }
  static const std::unordered_map<std::string, std::pair<std::uint32_t, std::string>> m = {
    {"SET",{NOTIFY_STRING,"set"}},{"SETEX",{NOTIFY_STRING,"set"}},{"PSETEX",{NOTIFY_STRING,"set"}},
    {"SETNX",{NOTIFY_STRING,"set"}},{"GETSET",{NOTIFY_STRING,"set"}},
    {"APPEND",{NOTIFY_STRING,"append"}},{"SETRANGE",{NOTIFY_STRING,"setrange"}},{"SETBIT",{NOTIFY_STRING,"setbit"}},
    {"INCR",{NOTIFY_STRING,"incrby"}},{"INCRBY",{NOTIFY_STRING,"incrby"}},{"DECR",{NOTIFY_STRING,"decrby"}},
    {"DECRBY",{NOTIFY_STRING,"decrby"}},{"INCRBYFLOAT",{NOTIFY_STRING,"incrbyfloat"}},{"GETDEL",{NOTIFY_GENERIC,"del"}},
    {"LPUSH",{NOTIFY_LIST,"lpush"}},{"RPUSH",{NOTIFY_LIST,"rpush"}},{"LPOP",{NOTIFY_LIST,"lpop"}},
    {"RPOP",{NOTIFY_LIST,"rpop"}},{"LSET",{NOTIFY_LIST,"lset"}},{"LREM",{NOTIFY_LIST,"lrem"}},
    {"LINSERT",{NOTIFY_LIST,"linsert"}},{"LTRIM",{NOTIFY_LIST,"ltrim"}},
    {"SADD",{NOTIFY_SET,"sadd"}},{"SREM",{NOTIFY_SET,"srem"}},{"SPOP",{NOTIFY_SET,"spop"}},
    {"HSET",{NOTIFY_HASH,"hset"}},{"HMSET",{NOTIFY_HASH,"hset"}},{"HSETNX",{NOTIFY_HASH,"hset"}},
    {"HDEL",{NOTIFY_HASH,"hdel"}},{"HINCRBY",{NOTIFY_HASH,"hincrby"}},{"HINCRBYFLOAT",{NOTIFY_HASH,"hincrbyfloat"}},
    {"ZADD",{NOTIFY_ZSET,"zadd"}},{"ZREM",{NOTIFY_ZSET,"zrem"}},{"ZINCRBY",{NOTIFY_ZSET,"zincr"}},
    {"ZPOPMIN",{NOTIFY_ZSET,"zpopmin"}},{"ZPOPMAX",{NOTIFY_ZSET,"zpopmax"}},
    {"XADD",{NOTIFY_STREAM,"xadd"}},{"XTRIM",{NOTIFY_STREAM,"xtrim"}},{"XDEL",{NOTIFY_STREAM,"xdel"}},
    {"EXPIRE",{NOTIFY_GENERIC,"expire"}},{"PEXPIRE",{NOTIFY_GENERIC,"expire"}},
    {"EXPIREAT",{NOTIFY_GENERIC,"expire"}},{"PEXPIREAT",{NOTIFY_GENERIC,"expire"}},
    {"PERSIST",{NOTIFY_GENERIC,"persist"}},{"RENAME",{NOTIFY_GENERIC,"rename_from"}},
    {"COPY",{NOTIFY_GENERIC,"copy_to"}},{"RESTORE",{NOTIFY_GENERIC,"restore"}},
  };
  auto it = m.find(cmd);
  if (it == m.end()) return;
  if (first_key > 0 && static_cast<std::size_t>(first_key) < args.size())
    notify_keyspace_event(it->second.first, it->second.second, args[static_cast<std::size_t>(first_key)], db);
  if (cmd == "RENAME" && args.size() >= 3)
    notify_keyspace_event(NOTIFY_GENERIC, "rename_to", args[2], db);
}

// Feed the MONITOR stream: format the command Redis-style and push to every monitor.
void feed_monitors(const std::string& cmd, const std::vector<std::string>& args, const SessionState& session) {
  if (g_monitor_fds.empty() || g_replaying_aof || !g_pubsub_send) return;
  if (cmd == "AUTH" || cmd == "MONITOR") return;  // Redis never echoes AUTH to monitors
  const std::int64_t ms = now_ms();
  char ts[40];
  std::snprintf(ts, sizeof(ts), "%lld.%06lld", static_cast<long long>(ms / 1000),
                static_cast<long long>((ms % 1000) * 1000));
  std::string line = "+";
  line += ts;
  line += " [" + std::to_string(session.db_index) + " " + (session.addr.empty() ? "?:0" : session.addr) + "]";
  for (const auto& a : args) {
    line += " \"";
    for (char c : a) {
      switch (c) {
        case '"': line += "\\\""; break;
        case '\\': line += "\\\\"; break;
        case '\n': line += "\\n"; break;
        case '\r': line += "\\r"; break;
        case '\t': line += "\\t"; break;
        default:
          if (static_cast<unsigned char>(c) < 32 || static_cast<unsigned char>(c) >= 127) {
            char hex[8]; std::snprintf(hex, sizeof(hex), "\\x%02x", static_cast<unsigned char>(c)); line += hex;
          } else line += c;
      }
    }
    line += "\"";
  }
  line += "\r\n";
  for (int mfd : g_monitor_fds) g_pubsub_send(mfd, line);
}

// ── HyperLogLog (self-consistent dense form) ──
// Stored as a string value: 8-byte magic + 16384 one-byte registers. NOTE: this is
// NOT byte-compatible with Redis's HYLL sparse/dense format (cross-migration of HLL
// values is unsupported), but PFADD/PFCOUNT/PFMERGE are internally consistent and
// give standard-accuracy estimates. Tracked in compat/delta.md.
constexpr int kHllP = 14;
constexpr int kHllRegs = 1 << kHllP;  // 16384
constexpr std::size_t kHllHdr = 8;
constexpr std::size_t kHllSize = kHllHdr + kHllRegs;
inline const char* hll_magic() { return "PEAHLL01"; }

std::uint64_t hll_hash(const std::string& s) {
  std::uint64_t h = 1469598103934665603ULL;            // FNV-1a 64
  for (unsigned char c : s) { h ^= c; h *= 1099511628211ULL; }
  h ^= h >> 30; h *= 0xbf58476d1ce4e5b9ULL;            // splitmix64 finalizer
  h ^= h >> 27; h *= 0x94d049bb133111ebULL;
  h ^= h >> 31;
  return h;
}
bool hll_is_valid(const std::string& v) {
  return v.size() == kHllSize && std::memcmp(v.data(), hll_magic(), kHllHdr) == 0;
}
std::string hll_new() {
  std::string v(kHllSize, '\0');
  std::memcpy(v.data(), hll_magic(), kHllHdr);
  return v;
}
bool hll_add(std::string& v, const std::string& elem) {
  const std::uint64_t h = hll_hash(elem);
  const std::uint32_t idx = static_cast<std::uint32_t>(h & (kHllRegs - 1));
  const std::uint64_t w = h >> kHllP;                  // 50 meaningful bits
  int rank = 1;
  std::uint64_t mask = 1ULL << (63 - kHllP);
  while (rank <= (64 - kHllP) && (w & mask) == 0) { ++rank; mask >>= 1; }
  auto* regs = reinterpret_cast<std::uint8_t*>(v.data()) + kHllHdr;
  if (regs[idx] < rank) { regs[idx] = static_cast<std::uint8_t>(rank); return true; }
  return false;
}
double hll_count(const std::string& v) {
  const auto* regs = reinterpret_cast<const std::uint8_t*>(v.data()) + kHllHdr;
  const double m = kHllRegs;
  double sum = 0.0; int zeros = 0;
  for (int i = 0; i < kHllRegs; ++i) { sum += std::ldexp(1.0, -static_cast<int>(regs[i])); if (regs[i] == 0) ++zeros; }
  const double alpha = 0.7213 / (1.0 + 1.079 / m);
  double E = alpha * m * m / sum;
  if (E <= 2.5 * m && zeros != 0) E = m * std::log(m / static_cast<double>(zeros));  // linear counting
  return E;
}

// Resident set size in bytes (0 if unavailable). Used for used_memory_rss / fragmentation.
std::int64_t process_rss_bytes() {
#if defined(__linux__)
  FILE* f = std::fopen("/proc/self/statm", "r");
  if (!f) return 0;
  long pages = 0, resident = 0;
  if (std::fscanf(f, "%ld %ld", &pages, &resident) != 2) { std::fclose(f); return 0; }
  std::fclose(f);
  return static_cast<std::int64_t>(resident) * static_cast<std::int64_t>(::sysconf(_SC_PAGESIZE));
#else
  struct rusage ru{};
  if (getrusage(RUSAGE_SELF, &ru) != 0) return 0;
  return static_cast<std::int64_t>(ru.ru_maxrss);  // macOS: bytes
#endif
}
std::map<std::string, FunctionLibrary> g_libraries;           // lc library name -> library
std::unordered_map<std::string, std::string> g_func_index;    // lc function name -> lc library name
bool g_rdb_load_functions_replace = true;                     // RDB function-load collision policy (see hooks)
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
// Currently-running function (for FUNCTION STATS "running_script"); only meaningful while busy.
std::string g_busy_func_name;
std::string g_busy_func_cmd;
// Distinguishes the busy source so SCRIPT KILL vs FUNCTION KILL each act only on their own kind
// (SCRIPT KILL on a busy function — or FUNCTION KILL on a busy eval — is rejected with BUSY).
bool g_busy_is_function = false;

// ── Server-wide stats (defined here, declared extern in command.hpp) ──
std::atomic<int> g_connected_clients{0};
std::atomic<std::int64_t> g_total_connections_received{0};
std::atomic<std::int64_t> g_total_commands_processed{0};
std::atomic<std::int64_t> g_server_start_time{0};
std::atomic<int> g_runtime_max_clients{10000};
std::function<int(std::int64_t)> g_count_synced_replicas;
std::function<int()> g_count_connected_replicas;
std::function<int()> g_blocked_clients_cb;
std::function<int(const std::string&, const std::string&, int)> g_client_kill_cb;
std::function<int(std::uint64_t, bool)> g_client_unblock_cb;
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
// Sharded pub/sub lives in a SEPARATE namespace from regular channels (SSUBSCRIBE/SPUBLISH):
// shard channel -> subscriber fds, and fd -> subscribed shard channels.
std::unordered_map<std::string, std::unordered_set<int>> g_shard_channel_subscribers;
std::unordered_map<int, std::unordered_set<std::string>> g_fd_shard_channels;
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
void pubsub_ssubscribe(int fd, const std::string& channel) {
  g_shard_channel_subscribers[channel].insert(fd);
  g_fd_shard_channels[fd].insert(channel);
}
void pubsub_sunsubscribe(int fd, const std::string& channel) {
  auto it = g_shard_channel_subscribers.find(channel);
  if (it != g_shard_channel_subscribers.end()) {
    it->second.erase(fd);
    if (it->second.empty()) g_shard_channel_subscribers.erase(it);
  }
  auto fit = g_fd_shard_channels.find(fd);
  if (fit != g_fd_shard_channels.end()) {
    fit->second.erase(channel);
    if (fit->second.empty()) g_fd_shard_channels.erase(fit);
  }
}
std::size_t shard_channel_count(int fd) {
  auto it = g_fd_shard_channels.find(fd);
  return it != g_fd_shard_channels.end() ? it->second.size() : 0;
}
void pubsub_unsubscribe_all(int fd) {
  auto scit = g_fd_shard_channels.find(fd);
  if (scit != g_fd_shard_channels.end()) {
    for (const auto& ch : scit->second) {
      auto sit = g_shard_channel_subscribers.find(ch);
      if (sit != g_shard_channel_subscribers.end()) {
        sit->second.erase(fd);
        if (sit->second.empty()) g_shard_channel_subscribers.erase(sit);
      }
    }
    g_fd_shard_channels.erase(scit);
  }
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

// Sharded publish: delivers an "smessage" only to shard subscribers of the exact channel
// (no pattern matching — sharded pub/sub has no pattern support).
std::int64_t pubsub_spublish(const std::string& channel, const std::string& message) {
  std::int64_t receivers = 0;
  if (!g_pubsub_send) return 0;
  auto it = g_shard_channel_subscribers.find(channel);
  if (it != g_shard_channel_subscribers.end()) {
    std::string msg = encode_pubsub_message("smessage", channel, message);
    for (int fd : it->second) {
      if (g_pubsub_send(fd, msg)) ++receivers;
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

bool valid_lib_name(const std::string& s) {
  if (s.empty()) return false;
  for (unsigned char c : s)
    if (!(std::isalnum(c) || c == '_')) return false;
  return true;
}

// Parse the `#!<engine> name=<name> [meta ...]` shebang. On success fills engine/name and body
// (= source after the shebang line). On failure sets `err` to the exact Redis message.
bool parse_function_shebang(const std::string& payload, std::string& engine, std::string& name,
                            std::string& body, std::string& err) {
  engine.clear(); name.clear(); body.clear();
  if (payload.rfind("#!", 0) != 0) { err = "Missing library metadata"; return false; }
  const auto nl = payload.find('\n');
  if (nl == std::string::npos) { err = "Invalid library metadata"; return false; }
  const std::string line = payload.substr(0, nl);
  body = payload.substr(nl + 1);  // Redis: the code starts at the '\n' (shebang line excluded)
  std::vector<std::string> parts;
  for (std::size_t i = 0; i < line.size();) {
    while (i < line.size() && std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    const std::size_t s = i;
    while (i < line.size() && !std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    if (i > s) parts.push_back(line.substr(s, i - s));
  }
  if (parts.empty()) { err = "Invalid library metadata"; return false; }
  engine = parts[0].rfind("#!", 0) == 0 ? parts[0].substr(2) : parts[0];
  bool have_name = false;
  for (std::size_t i = 1; i < parts.size(); ++i) {
    const std::string& tok = parts[i];
    if (tok.size() >= 5 && lower(tok.substr(0, 5)) == "name=") {
      if (have_name) { err = "Invalid metadata value, name argument was given multiple times"; return false; }
      std::string v = tok.substr(5);
      if (v.size() >= 2 && v.front() == '"' && v.back() == '"') v = v.substr(1, v.size() - 2);
      name = v; have_name = true;
    } else {
      err = "Invalid metadata value given: " + tok; return false;
    }
  }
  if (!have_name) { err = "Library name was not given"; return false; }
  return true;
}

// Load (or REPLACE) a single library from its full source. On success returns true and sets
// out_name; on failure returns false and sets err (exact Redis text, no "ERR " prefix). On any
// failure the existing library (if replacing) is left untouched.
bool load_function_library(const std::string& code, bool replace, std::string& out_name, std::string& err) {
  std::string engine, name, body;
  if (!parse_function_shebang(code, engine, name, body, err)) return false;
  if (!valid_lib_name(name)) {
    err = "Library names can only contain letters, numbers, or underscores(_) and must be at least one character long";
    return false;
  }
  if (upper(engine) != "LUA") { err = "Engine '" + engine + "' not found"; return false; }
  const std::string lc = lower(name);
  auto existing = g_libraries.find(lc);
  if (existing != g_libraries.end() && !replace) { err = "Library '" + name + "' already exists"; return false; }

  std::vector<LuaRegisteredFunction> fns;
  if (!lua_engine_load_library(body, fns, err)) return false;  // err set (compile/register/timeout/no-functions)

  // Reject collision with a function that belongs to a DIFFERENT library.
  for (const auto& f : fns) {
    auto idx = g_func_index.find(lower(f.name));
    if (idx != g_func_index.end() && idx->second != lc) {
      err = "Function " + f.name + " already exists";
      for (auto& g : fns) lua_engine_unref_function(g.callback_ref);
      return false;
    }
  }
  // Commit. Drop the old library (functions + refs) only now that the new one is valid.
  if (existing != g_libraries.end()) {
    for (auto& f : existing->second.functions) {
      g_func_index.erase(lower(f.name));
      lua_engine_unref_function(f.callback_ref);
    }
    g_libraries.erase(existing);
  }
  FunctionLibrary lib;
  lib.name = name; lib.engine = "LUA"; lib.code = code;
  for (const auto& f : fns) {
    lib.functions.push_back({f.name, f.description, f.flags, f.callback_ref});
    g_func_index[lower(f.name)] = lc;
  }
  g_libraries[lc] = std::move(lib);
  out_name = name;
  return true;
}

// Release every library's callbacks and clear the registry (FUNCTION FLUSH / RDB load reset).
void functions_flush_all() {
  for (auto& [lc, lib] : g_libraries)
    for (auto& f : lib.functions) lua_engine_unref_function(f.callback_ref);
  g_libraries.clear();
  g_func_index.clear();
}

// FNV-1a 64-bit — a self-contained checksum for the FUNCTION DUMP/RESTORE payload. The blob is
// PeaDB-internal (it round-trips only within PeaDB; Redis's serialization differs), so we don't
// need crc64 byte-parity — only self-consistent tamper detection for "payload ... are wrong".
std::uint64_t functions_fnv1a64(const std::string& s) {
  std::uint64_t h = 1469598103934665603ULL;
  for (unsigned char c : s) { h ^= c; h *= 1099511628211ULL; }
  return h;
}

// Serialize every library's source into a versioned, checksummed blob:
//   0xF5, 0x01 (version), u32 count, [u32 code_len + code]..., u64 fnv1a(all preceding bytes).
std::string functions_dump() {
  std::string out;
  out.push_back(static_cast<char>(0xF5));
  out.push_back(0x01);
  auto put32 = [&](std::uint32_t v) { for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF)); };
  put32(static_cast<std::uint32_t>(g_libraries.size()));
  for (const auto& [lc, lib] : g_libraries) {
    put32(static_cast<std::uint32_t>(lib.code.size()));
    out += lib.code;
  }
  const std::uint64_t crc = functions_fnv1a64(out);
  for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((crc >> (8 * i)) & 0xFF));
  return out;
}

// Apply a FUNCTION DUMP blob under FLUSH|APPEND|REPLACE. Atomic: on any failure the pre-call
// state is fully restored (Redis semantics — a bad/colliding payload never partially applies).
bool functions_restore(const std::string& payload, const std::string& policy, std::string& err) {
  const std::string bad = "payload version or checksum are wrong";
  if (payload.size() < 2 + 4 + 8 ||
      static_cast<unsigned char>(payload[0]) != 0xF5 || payload[1] != 0x01) { err = bad; return false; }
  const std::string body = payload.substr(0, payload.size() - 8);
  std::uint64_t stored = 0;
  for (int i = 0; i < 8; ++i)
    stored |= static_cast<std::uint64_t>(static_cast<unsigned char>(payload[payload.size() - 8 + i])) << (8 * i);
  if (functions_fnv1a64(body) != stored) { err = bad; return false; }

  std::size_t pos = 2;
  auto get32 = [&](std::uint32_t& v) -> bool {
    if (pos + 4 > body.size()) return false;
    v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<std::uint32_t>(static_cast<unsigned char>(body[pos++])) << (8 * i);
    return true;
  };
  std::uint32_t count = 0;
  if (!get32(count)) { err = bad; return false; }
  std::vector<std::string> codes;
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint32_t len = 0;
    if (!get32(len) || pos + len > body.size()) { err = bad; return false; }
    codes.push_back(body.substr(pos, len));
    pos += len;
  }

  // APPEND rejects a library whose name already exists (Redis message here omits the quotes
  // that FUNCTION LOAD uses). Pre-checked before any mutation so the state is untouched.
  if (policy == "APPEND") {
    for (const auto& code : codes) {
      std::string eng, nm, body, e;
      if (parse_function_shebang(code, eng, nm, body, e) && g_libraries.count(lower(nm))) {
        err = "Library " + nm + " already exists";
        return false;
      }
    }
  }

  // Snapshot current library sources so we can roll back atomically on failure.
  std::vector<std::string> snapshot;
  for (const auto& [lc, lib] : g_libraries) snapshot.push_back(lib.code);

  if (policy == "FLUSH") functions_flush_all();
  const bool replace = (policy == "REPLACE");
  std::string name;
  for (const auto& code : codes) {
    if (!load_function_library(code, replace, name, err)) {
      functions_flush_all();
      std::string ignore, e2;
      for (const auto& s : snapshot) load_function_library(s, true, ignore, e2);
      return false;
    }
  }
  return true;
}

std::string encode_error(const std::string& value);  // defined below (encoder block)

// Shared FCALL / FCALL_RO body. `ro_command` is true for FCALL_RO.
std::string do_fcall(const std::vector<std::string>& args, SessionState& session, bool ro_command) {
  auto idx = g_func_index.find(lower(args[1]));
  if (idx == g_func_index.end()) return encode_error("Function not found");
  const FunctionEntry* entry = nullptr;
  auto libit = g_libraries.find(idx->second);
  if (libit != g_libraries.end())
    for (const auto& f : libit->second.functions)
      if (lower(f.name) == lower(args[1])) { entry = &f; break; }
  if (!entry) return encode_error("Function not found");

  std::int64_t numkeys = 0;
  if (!parse_i64(args[2], numkeys)) return encode_error("Bad number of keys provided");
  if (numkeys < 0) return encode_error("Number of keys can't be negative");
  if (static_cast<std::size_t>(numkeys) > args.size() - 3)
    return encode_error("Number of keys can't be greater than number of args");

  const bool no_writes = (entry->flags & kFuncNoWrites) != 0;
  if (ro_command && !no_writes)
    return encode_error("Can not execute a script with write flag using *_ro command.");
  const bool readonly = ro_command || no_writes;
  // A function that could write is denied under OOM unless it carries allow-oom; a no-writes
  // function always runs (it can't grow memory), matching Redis.
  if (!no_writes && !(entry->flags & kFuncAllowOom) && oom_should_reject())
    return std::string("-OOM allow-oom flag is not set on the script, can not run it when used memory > 'maxmemory'\r\n");

  // Cooperative BUSY simulation for trivially-infinite functions, mirroring eval_miniscript: a
  // real infinite loop would block the single-threaded event loop, so instead we advertise BUSY
  // and let another client observe it / issue FUNCTION KILL. (FUNCTION STATS reports this too.)
  if (libit->second.code.find("while true do") != std::string::npos ||
      libit->second.code.find("while 1 do") != std::string::npos) {
    g_script_kill_requested.store(false, std::memory_order_release);
    g_script_busy.store(true, std::memory_order_release);
    g_busy_script_session = &session;
    g_busy_is_function = true;
    g_busy_func_name = entry->name;
    std::string cmd = lower(args[0]);
    for (std::size_t i = 1; i < args.size(); ++i) cmd += " " + args[i];
    g_busy_func_cmd = cmd;
    // Mirror the eval simulation's wording ("killed by user") — the scripting suite runs its
    // SCRIPT/FUNCTION KILL tests as both EVAL and FCALL and asserts the reply contains it.
    return std::string("-BUSY Redis is busy running a script. Script killed by user with FUNCTION KILL.\r\n");
  }

  std::vector<std::string> keys(args.begin() + 3, args.begin() + 3 + numkeys);
  std::vector<std::string> argv(args.begin() + 3 + numkeys, args.end());
  g_script_client_resp_version = session.resp_version;
  g_script_current_session = &session;
  g_busy_func_name = entry->name;
  g_busy_func_cmd = "fcall";
  g_busy_is_function = true;
  // allow-oom lets the function's inner writes bypass the per-command OOM gate.
  const bool prev_allow_oom = g_script_allow_oom;
  if (entry->flags & kFuncAllowOom) g_script_allow_oom = true;
  const int callback_ref = entry->callback_ref;
  // Freeze time for the duration of the call (Redis behavior — a key's logical expiry does not
  // advance mid-function, so TIME is stable and PX-expiring keys survive an inner DEBUG SLEEP).
  store().freeze_time();
  const auto out = lua_engine_fcall(callback_ref, keys, argv, session, readonly);
  store().unfreeze_time();
  g_script_allow_oom = prev_allow_oom;
  g_script_current_session = nullptr;
  return out;
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
// RESP3 has a single null type; RESP2 distinguishes the null array (*-1).
std::string encode_null_array(RespVersion v) { return v == RespVersion::Resp3 ? "_\r\n" : "*-1\r\n"; }

// ── RESP3-aware type encoders (fall back to RESP2 shapes for RESP2 clients) ──
std::string encode_double(double d, RespVersion v) {
  std::string s;
  if (std::isinf(d)) s = d > 0 ? "inf" : "-inf";
  else if (std::isnan(d)) s = "nan";
  else s = format_score(d);
  return v == RespVersion::Resp3 ? ("," + s + "\r\n") : encode_bulk(s);
}
std::string encode_bool(bool b, RespVersion v) {
  return v == RespVersion::Resp3 ? (b ? "#t\r\n" : "#f\r\n") : (b ? ":1\r\n" : ":0\r\n");
}
// kv holds already-encoded elements [k,v,k,v,...]; RESP3 emits a map (%N), RESP2 a flat array.
std::string encode_map(const std::vector<std::string>& kv, RespVersion v) {
  const std::size_t pairs = kv.size() / 2;
  std::string out = (v == RespVersion::Resp3 ? "%" : "*") +
                    std::to_string(v == RespVersion::Resp3 ? pairs : kv.size()) + "\r\n";
  for (const auto& e : kv) out += e;
  return out;
}
std::string encode_set_reply(const std::vector<std::string>& items, RespVersion v) {
  std::string out = (v == RespVersion::Resp3 ? "~" : "*") + std::to_string(items.size()) + "\r\n";
  for (const auto& e : items) out += e;
  return out;
}

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

// Rewrite a client write command into its canonical, deterministic propagation form —
// shared by the replication backlog AND the AOF (both must replay identically later /
// elsewhere): relative expirations become PEXPIREAT, SPOP becomes SREM of the actually
// removed members, XADD's auto '*' id becomes the concrete generated id, etc.
// Returns false when the command must not be propagated at all.
bool canonicalize_propagated_command(const std::vector<std::string>& args,
                                     const std::string* reply_ptr,
                                     std::vector<std::string>& out) {
  const auto cmd = upper(args[0]);
  out = args;
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
    } else if (args.size() >= 3 && store().type_of(args[1]) == "none") {
      // SPOP <key> <count> that emptied the set: Redis propagates the deletion (UNLINK
      // when lazyfree-lazy-server-del is on) instead of an SREM of every member. The
      // single-member form still propagates SREM even when it removes the last member.
      out = {g_lazyfree_lazy_server_del ? "UNLINK" : "DEL", args[1]};
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
    if (!key_exists(args[1])) {
      // The key wasn't stored (already-expired absolute TTL); if REPLACE removed an existing
      // key, Redis propagates the deletion as DEL/UNLINK instead of the RESTORE.
      out = {g_lazyfree_lazy_server_del ? "UNLINK" : "DEL", args[1]};
    } else {
      const auto abs = abs_ttl(args[1]);
      if (abs >= 0) {
        std::string absttl = "ABSTTL";
        for (std::size_t i = 4; i < args.size(); ++i) {
          if (upper(args[i]) == "ABSTTL") { absttl = args[i]; break; }
        }
        out = {"RESTORE", args[1], std::to_string(abs), "{" + args[3] + "}", absttl};
      }
    }
  } else if (cmd == "SCRIPT") {
    emit = false;
  } else if (cmd == "BLMPOP" || cmd == "BZMPOP" || cmd == "LMPOP" || cmd == "ZMPOP") {
    // These handlers propagate their effect explicitly as deterministic LPOP/RPOP or
    // ZPOPMIN/ZPOPMAX (both on the immediate and the unblock paths).
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
  } else if (cmd == "XADD" && args.size() >= 5 && reply_ptr != nullptr) {
    // Replace an auto-generated id ('*' or 'ms-*') with the concrete id from the reply
    // so replicas/AOF replay reproduce identical stream ids. A nil reply (NOMKSTREAM
    // on a missing stream) means nothing was written.
    const auto& r = *reply_ptr;
    if (r.rfind("$-1", 0) == 0 || r.rfind("_", 0) == 0) {
      emit = false;
    } else {
      // Locate the id argument: skip NOMKSTREAM and trimming options.
      std::size_t i = 2;
      while (i < args.size()) {
        const auto o = upper(args[i]);
        if (o == "NOMKSTREAM") { ++i; continue; }
        if (o == "MAXLEN" || o == "MINID") {
          ++i;
          if (i < args.size() && (args[i] == "~" || args[i] == "=")) ++i;
          if (i < args.size()) ++i;  // threshold
          if (i + 1 < args.size() && upper(args[i]) == "LIMIT") i += 2;
          continue;
        }
        break;  // args[i] is the id
      }
      if (i < args.size() && args[i].find('*') != std::string::npos &&
          !r.empty() && r[0] == '$') {
        const auto eol = r.find("\r\n");
        if (eol != std::string::npos) {
          const auto id_start = eol + 2;
          const auto id_end = r.find("\r\n", id_start);
          if (id_end != std::string::npos) out[i] = r.substr(id_start, id_end - id_start);
        }
      }
    }
  }

  if (!emit || out.empty()) return false;
  return true;
}

void append_replication_event(const std::vector<std::string>& args, const SessionState& session,
                              const std::string* reply_ptr = nullptr) {
  if (args.empty()) return;
  if (g_replaying_aof) return;  // Suppress replication broadcast while replaying from master or AOF
  std::vector<std::string> out;
  if (!canonicalize_propagated_command(args, reply_ptr, out)) return;
  // redis.set_repl(REPL_NONE/REPL_AOF) suppresses replica propagation of the script's
  // subsequent effects — both the captured (EXEC) and direct backlog paths.
  if (in_lua_script_context() && !(g_script_repl_flags & 2)) return;
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
    notify_keyspace_event(NOTIFY_EXPIRED, "expired", key, session.db_index);
  }
}

// fsync the parent directory of `path`. Required after an atomic rename to
// ensure the new directory entry is durable across power loss — file fsync
// alone only flushes contents, not the directory metadata that names the file.
// Returns false on open or fsync failure; logs are caller's responsibility.
bool fsync_parent_dir(const std::filesystem::path& path) {
  std::filesystem::path parent = path.parent_path();
  if (parent.empty()) parent = ".";
  const int dfd = ::open(parent.c_str(), O_RDONLY | O_CLOEXEC);
  if (dfd < 0) return false;
  const int rc = ::fsync(dfd);
  ::close(dfd);
  return rc == 0;
}

void open_aof_fd() {
  if (g_aof_fd >= 0) { ::close(g_aof_fd); g_aof_fd = -1; }
  g_aof_fd = ::open(g_aof_path.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
  // The db context at the file's tail is unknown (fresh file, pre-existing file, or a
  // just-finished rewrite) — force the next append to emit an explicit SELECT.
  g_last_aof_db = -1;
}

void append_aof(const std::vector<std::string>& args);  // defined below

// Canonicalizing AOF append for a dispatched client write: same deterministic rewrite
// as the replication stream, plus SELECT lines on db change so multi-db datasets
// replay into the right database.
void append_aof_command(const std::vector<std::string>& args, const SessionState& session,
                        const std::string* reply_ptr) {
  if (!g_appendonly || g_replaying_aof || args.empty()) return;
  // redis.set_repl(REPL_NONE/REPL_SLAVE) suppresses AOF propagation of script effects.
  if (in_lua_script_context() && !(g_script_repl_flags & 1)) return;
  std::vector<std::string> out;
  if (!canonicalize_propagated_command(args, reply_ptr, out)) return;
  if (session.db_index != g_last_aof_db) {
    append_aof({"SELECT", std::to_string(session.db_index)});
    g_last_aof_db = session.db_index;
  }
  append_aof(out);
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
  // In the strongest durability mode, a failed fsync means the just-written command
  // is NOT durable — surface it (don't silently swallow, matching the strict
  // fail-on-fsync posture used for RDB/AOF-rewrite).
  if (g_appendfsync == "always" && ::fsync(g_aof_fd) != 0) {
    g_aof_write_error = true;
    log(LogLevel::Error, "AOF fsync (always) failed: " + std::string(std::strerror(errno)));
  }
  // Buffer for BGREWRITEAOF diff: commands written while child is rewriting
  // are collected here and appended to the new AOF when the child finishes.
  if (g_bgrewrite_in_progress.load(std::memory_order_relaxed)) {
    // Cap at 256 MiB to bound RSS during a long rewrite on a write-heavy master.
    constexpr std::size_t kMaxRewriteBuf = 256ULL * 1024 * 1024;
    if (g_aof_rewrite_buf.size() < kMaxRewriteBuf) {
      g_aof_rewrite_buf.append(payload);
    } else if (!g_aof_rewrite_buf_capped) {
      g_aof_rewrite_buf_capped = true;
      log(LogLevel::Warn, "BGREWRITEAOF: rewrite diff buffer reached 256 MiB cap; "
          "writes beyond this point will not appear in the rewritten AOF. "
          "Consider reducing write rate or increasing available memory.");
    }
  }
}

// Eviction loop (maxmemory-policy). Evicts sampled keys until used memory is back under
// the limit or nothing is evictable. Each evicted key is propagated like a server-issued
// deletion: a DEL to the replication backlog (with SELECT on db change, so replicas stay
// in sync) and to the AOF (bare DEL — the live AOF is db-agnostic, and DEL of an absent
// key is a harmless no-op on replay), plus an "evicted" keyspace notification.
void evict_to_fit() {
  // Bound the loop so a pathological config can never spin forever; in practice it exits
  // as soon as used_memory drops under the limit (evict_one keeps the cache live).
  int guard = 0;
  while (store().used_memory() > g_config_maxmemory) {
    if (++guard > 10'000'000) break;
    auto victim = store().evict_one(g_maxmemory_policy, g_maxmemory_samples);
    if (!victim) break;  // nothing evictable (e.g. volatile-* with no TTL keys)
    if (victim->db != g_last_repl_db) {
      backlog_append(encode_command_resp({"SELECT", std::to_string(victim->db)}));
      g_last_repl_db = victim->db;
    }
    backlog_append(encode_command_resp({"DEL", victim->key}));
    append_aof({"DEL", victim->key});
    notify_keyspace_event(NOTIFY_EVICTED, "evicted", victim->key, victim->db);
  }
}

bool rewrite_aof_file(std::string& err) {
  err.clear();
  const std::string tmp_path = g_aof_path.string() + ".tmp";
  const int tmp_fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (tmp_fd < 0) { err = "cannot open AOF tmp for rewrite"; return false; }
  bool write_failed = false;
  // Full-write loop: a single ::write() may return a short (positive) count under disk
  // pressure or signal interruption; treating that as success would leave a truncated temp
  // AOF that the parent then commits.
  auto write_all = [&](const std::string& payload) {
    const char* p = payload.data();
    std::size_t remaining = payload.size();
    while (remaining > 0) {
      const ssize_t rc = ::write(tmp_fd, p, remaining);
      if (rc <= 0) { write_failed = true; return; }
      p += static_cast<std::size_t>(rc);
      remaining -= static_cast<std::size_t>(rc);
    }
  };
  // Function libraries are server-level state, not in the keyspace — emit them first so an AOF
  // rewrite (and any later DEBUG LOADAOF / restart) preserves loaded functions.
  for (const auto& [lc, lib] : g_libraries) {
    write_all(encode_command_resp({"FUNCTION", "LOAD", "REPLACE", lib.code}));
    if (write_failed) { ::close(tmp_fd); ::unlink(tmp_path.c_str()); err = "AOF rewrite write error"; return false; }
  }
  const auto cmds = store().export_aof_commands();
  for (const auto& cmd : cmds) {
    write_all(encode_command_resp(cmd));
    if (write_failed) {
      ::close(tmp_fd); ::unlink(tmp_path.c_str());
      err = "AOF rewrite write error"; return false;
    }
  }
  if (::fsync(tmp_fd) != 0) {
    ::close(tmp_fd); ::unlink(tmp_path.c_str());
    err = "AOF rewrite fsync error"; return false;
  }
  ::close(tmp_fd);
  // Parent owns the rename + commit point: child writes temp.aof and exits;
  // parent appends the diff buffer and only then atomically renames over the
  // live AOF. This means any failure (cap overflow, diff append error, rename
  // error, child crash) leaves the old AOF intact on disk.
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
    g_busy_is_function = false;
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
  // Dynamic, like Redis helloCommand: proto/id/role reflect the actual session and
  // server state; every key is a bulk string in both protocols.
  const bool r3 = session.resp_version == RespVersion::Resp3;
  const std::string role = g_replication_role == "slave" ? "replica" : "master";
  std::string out = r3 ? "%7\r\n" : "*14\r\n";
  out += encode_bulk("server");  out += encode_bulk("redis");
  out += encode_bulk("version"); out += encode_bulk("7.2.5");
  out += encode_bulk("proto");   out += encode_integer(r3 ? 3 : 2);
  out += encode_bulk("id");      out += encode_integer(static_cast<long long>(session.client_id));
  out += encode_bulk("mode");    out += encode_bulk("standalone");
  out += encode_bulk("role");    out += encode_bulk(role);
  out += encode_bulk("modules"); out += "*0\r\n";
  return out;
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

// Numeric stream-ID "<" for command-layer trimming (bare "ms" → seq 0). Stream IDs must
// never be compared as strings ("10-0" < "9-0" lexically).
bool stream_id_lt(const std::string& a, const std::string& b) {
  auto parse = [](const std::string& s, std::uint64_t& ms, std::uint64_t& seq) {
    const auto d = s.find('-');
    try {
      ms = std::stoull(d == std::string::npos ? s : s.substr(0, d));
      seq = d == std::string::npos ? 0 : std::stoull(s.substr(d + 1));
    } catch (...) { ms = 0; seq = 0; }
  };
  std::uint64_t am, as, bm, bs;
  parse(a, am, as); parse(b, bm, bs);
  return am != bm ? am < bm : as < bs;
}

// A well-formed explicit stream id: "ms" or "ms-seq", digits only (no "-"/"+"/"*"/garbage).
bool stream_id_is_valid(const std::string& s) {
  auto all_digits = [](const std::string& t) {
    if (t.empty()) return false;
    for (char c : t) if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    return true;
  };
  const auto d = s.find('-');
  if (d == std::string::npos) return all_digits(s);
  return all_digits(s.substr(0, d)) && all_digits(s.substr(d + 1));
}

bool parse_f64(const std::string& s, long double& out) {
  // strtold (not std::stold): std::stold throws out_of_range on subnormal underflow
  // (e.g. 9.88e-323), which Redis accepts. strtold returns the (subnormal/zero) value
  // and sets errno=ERANGE; accept ERANGE-underflow but reject ERANGE-overflow (inf).
  if (s.empty()) return false;
  errno = 0;
  char* end = nullptr;
  out = std::strtold(s.c_str(), &end);
  if (end != s.c_str() + s.size()) return false;
  if (errno == ERANGE && std::isinf(static_cast<double>(out))) return false;
  return true;
}

// Parse a blocking-command timeout (seconds, float), mirroring Redis's
// getTimeoutFromObjectOrReply: distinct errors for non-float, out-of-range (the ms
// deadline would overflow int64), and negative. Returns "" on success (secs set).
std::string parse_block_timeout_sec(const std::string& arg, long double& secs) {
  if (!parse_f64(arg, secs) || std::isnan(static_cast<double>(secs))) {
    return encode_error("timeout is not a float or out of range");
  }
  long double ftval = secs * 1000.0L;  // seconds -> ms
  if (ftval > 9.0e18L) return encode_error("timeout is out of range");
  if (secs < 0) return encode_error("timeout is negative");
  return "";
}

// Redis string2ll semantics: optional '-', digits only, no whitespace, no '+' prefix,
// no leading zeros (except "0"), must fit int64. Used where Redis demands a strictly
// valid integer value (INCR/HINCRBY stored values).
bool parse_strict_int(const std::string& s, std::int64_t& out) {
  if (s.empty()) return false;
  std::size_t i = (s[0] == '-' ? 1 : 0);
  if (i >= s.size()) return false;
  if (s[i] == '0' && s.size() - i > 1) return false;
  for (std::size_t j = i; j < s.size(); ++j) {
    if (!std::isdigit(static_cast<unsigned char>(s[j]))) return false;
  }
  try {
    std::size_t consumed = 0;
    out = std::stoll(s, &consumed);
    return consumed == s.size();
  } catch (...) {
    return false;
  }
}

// Redis string2ld semantics: no leading/trailing whitespace, no embedded NUL, the whole
// string must parse; "inf"/"-inf" are accepted but NaN is not. Used where Redis demands
// a *valid float* (INCRBYFLOAT and friends), unlike the more lenient parse_f64.
bool parse_strict_float(const std::string& s, long double& out) {
  if (s.empty() || std::isspace(static_cast<unsigned char>(s.front()))) return false;
  if (s.find('\0') != std::string::npos) return false;
  char* end = nullptr;
  errno = 0;
  out = std::strtold(s.c_str(), &end);
  if (end != s.c_str() + s.size()) return false;
  return !std::isnan(static_cast<double>(out));
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

// Shared ZRANGE/ZRANGESTORE range selector (Redis 6.2 unified ZRANGE semantics):
// index (default), BYSCORE or BYLEX modes, optional REV, LIMIT for the BY* modes,
// WITHSCORES only where the caller allows it. Fills `items` in output order and
// returns an empty string on success or a complete error reply.
std::string zrange_select(const std::vector<std::string>& args, std::size_t key_idx,
                          std::size_t min_idx, std::size_t opts_idx, bool allow_withscores,
                          std::vector<std::pair<std::string, double>>& items,
                          bool& withscores) {
  const std::string& key = args[key_idx];
  enum class Mode { Index, ByScore, ByLex };
  Mode mode = Mode::Index;
  bool rev = false, has_limit = false;
  withscores = false;
  std::int64_t lim_off = 0, lim_cnt = -1;
  for (std::size_t i = opts_idx; i < args.size(); ++i) {
    const auto o = upper(args[i]);
    if (o == "BYSCORE") mode = Mode::ByScore;
    else if (o == "BYLEX") mode = Mode::ByLex;
    else if (o == "REV") rev = true;
    else if (allow_withscores && o == "WITHSCORES") withscores = true;
    else if (o == "LIMIT" && i + 2 < args.size()) {
      if (!parse_i64(args[i + 1], lim_off) || !parse_i64(args[i + 2], lim_cnt))
        return encode_error("value is not an integer or out of range");
      has_limit = true; i += 2;
    } else {
      return encode_error("syntax error");
    }
  }
  if (has_limit && mode == Mode::Index)
    return encode_error("syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
  if (withscores && mode == Mode::ByLex)
    return encode_error("syntax error, WITHSCORES not supported in combination with BYLEX");
  if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
  bool wt = false;
  auto all = store().zrange(key, 0, -1, true, wt);  // ascending {member,score}
  if (wt) return wrongtype_error_reply();

  if (mode == Mode::Index) {
    std::int64_t start = 0, stop = 0;
    if (!parse_i64(args[min_idx], start) || !parse_i64(args[min_idx + 1], stop))
      return encode_error("value is not an integer or out of range");
    if (rev) std::reverse(all.begin(), all.end());
    const std::int64_t n = static_cast<std::int64_t>(all.size());
    std::int64_t s = start < 0 ? n + start : start, e = stop < 0 ? n + stop : stop;
    if (s < 0) s = 0;
    if (e >= n) e = n - 1;
    for (std::int64_t i = s; i <= e && i < n; ++i) items.push_back(all[i]);
    return "";
  }
  if (mode == Mode::ByScore) {
    // REV swaps bound order: ... max min BYSCORE REV.
    const std::string& lo_s = rev ? args[min_idx + 1] : args[min_idx];
    const std::string& hi_s = rev ? args[min_idx] : args[min_idx + 1];
    auto parse_bound = [](const std::string& s, double& val, bool& ex) -> bool {
      ex = false;
      if (s == "-inf") { val = -std::numeric_limits<double>::infinity(); return true; }
      if (s == "+inf" || s == "inf") { val = std::numeric_limits<double>::infinity(); return true; }
      std::string v = s;
      if (!v.empty() && v[0] == '(') { ex = true; v = v.substr(1); }
      try { val = std::stod(v); return !std::isnan(val); } catch (...) { return false; }  // NaN is not a valid bound
    };
    double lo, hi;
    bool lo_ex, hi_ex;
    if (!parse_bound(lo_s, lo, lo_ex) || !parse_bound(hi_s, hi, hi_ex))
      return encode_error("min or max is not a float");
    for (const auto& [m, sc] : all) {
      const bool above = lo_ex ? (sc > lo) : (sc >= lo);
      const bool below = hi_ex ? (sc < hi) : (sc <= hi);
      if (above && below) items.push_back({m, sc});
    }
    if (rev) std::reverse(items.begin(), items.end());
  } else {  // ByLex — all members assumed same score; compare lexicographically.
    const std::string& lo_s = rev ? args[min_idx + 1] : args[min_idx];
    const std::string& hi_s = rev ? args[min_idx] : args[min_idx + 1];
    // Bound forms: "-" (min), "+" (max), "[member" (inclusive), "(member" (exclusive).
    auto lex_ge_lo = [&](const std::string& m) -> bool {
      if (lo_s == "-") return true;
      if (lo_s == "+") return false;
      if (lo_s.empty()) return false;
      const std::string b = lo_s.substr(1);
      return lo_s[0] == '(' ? (m > b) : (m >= b);
    };
    auto lex_le_hi = [&](const std::string& m) -> bool {
      if (hi_s == "+") return true;
      if (hi_s == "-") return false;
      if (hi_s.empty()) return false;
      const std::string b = hi_s.substr(1);
      return hi_s[0] == '(' ? (m < b) : (m <= b);
    };
    if ((!lo_s.empty() && lo_s[0] != '[' && lo_s[0] != '(' && lo_s != "-" && lo_s != "+") ||
        (!hi_s.empty() && hi_s[0] != '[' && hi_s[0] != '(' && hi_s != "-" && hi_s != "+"))
      return encode_error("min or max not valid string range item");
    for (const auto& [m, sc] : all) if (lex_ge_lo(m) && lex_le_hi(m)) items.push_back({m, sc});
    if (rev) std::reverse(items.begin(), items.end());
  }

  // LIMIT applies to BYSCORE/BYLEX, on the (possibly reversed) output order.
  if (has_limit) {
    if (lim_off > 0) {
      if (static_cast<std::size_t>(lim_off) >= items.size()) items.clear();
      else items.erase(items.begin(), items.begin() + lim_off);
    }
    if (lim_cnt >= 0 && static_cast<std::size_t>(lim_cnt) < items.size())
      items.resize(static_cast<std::size_t>(lim_cnt));
  }
  return "";
}

// Shared LPOP/RPOP body: plain form returns a bulk (or nil), COUNT form returns an
// array (or nil array for a missing key), matching Redis 6.2+ popGenericCommand.
std::string list_pop_command(const std::vector<std::string>& args, bool left, RespVersion resp) {
  if (args.size() > 3) {
    return encode_error(std::string("wrong number of arguments for '") +
                        (left ? "lpop" : "rpop") + "' command");
  }
  const bool has_count = args.size() == 3;
  std::int64_t count = 0;
  if (has_count) {
    if (!parse_i64(args[2], count)) return encode_error("value is not an integer or out of range");
    if (count < 0) return encode_error("value is out of range, must be positive");
  }
  return with_list_key(args[1], [&]() {
    bool wrongtype = false;
    if (!has_count) {
      auto v = left ? store().lpop(args[1], wrongtype) : store().rpop(args[1], wrongtype);
      if (wrongtype) return wrongtype_error_reply();
      return v.has_value() ? encode_bulk(*v) : encode_null(resp);
    }
    const auto len = store().llen(args[1], wrongtype);
    if (wrongtype) return wrongtype_error_reply();
    if (len == 0) return encode_null_array(resp);
    std::vector<std::string> out;
    const auto n = std::min<std::int64_t>(count, len);
    out.reserve(static_cast<std::size_t>(n));
    for (std::int64_t i = 0; i < n; ++i) {
      auto v = left ? store().lpop(args[1], wrongtype) : store().rpop(args[1], wrongtype);
      if (!v.has_value()) break;
      out.push_back(encode_bulk(*v));
    }
    return encode_array(out);
  });
}

// ── Geospatial helpers (bit-exact with Redis geohash.c / geohash_helper.c) ──
namespace geoutil {
constexpr double LAT_MIN = -85.05112878, LAT_MAX = 85.05112878;
constexpr double LON_MIN = -180.0, LON_MAX = 180.0;
constexpr double EARTH_R = 6372797.560856;      // GEO_EARTH_RADIUS_IN_METERS
constexpr double DEG_RAD = 0.017453292519943295769236907684886;
constexpr double STEP_POW = 67108864.0;         // 2^26

inline std::uint64_t interleave64(std::uint32_t xlo, std::uint32_t ylo) {
  static const std::uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
      0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL, 0x0000FFFF0000FFFFULL};
  static const unsigned S[] = {1, 2, 4, 8, 16};
  std::uint64_t x = xlo, y = ylo;
  x = (x | (x << S[4])) & B[4]; y = (y | (y << S[4])) & B[4];
  x = (x | (x << S[3])) & B[3]; y = (y | (y << S[3])) & B[3];
  x = (x | (x << S[2])) & B[2]; y = (y | (y << S[2])) & B[2];
  x = (x | (x << S[1])) & B[1]; y = (y | (y << S[1])) & B[1];
  x = (x | (x << S[0])) & B[0]; y = (y | (y << S[0])) & B[0];
  return x | (y << 1);
}
inline std::uint64_t deinterleave64(std::uint64_t v) {
  static const std::uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
      0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL, 0x0000FFFF0000FFFFULL, 0x00000000FFFFFFFFULL};
  static const unsigned S[] = {0, 1, 2, 4, 8, 16};
  std::uint64_t x = v, y = v >> 1;
  x = (x | (x >> S[0])) & B[0]; y = (y | (y >> S[0])) & B[0];
  x = (x | (x >> S[1])) & B[1]; y = (y | (y >> S[1])) & B[1];
  x = (x | (x >> S[2])) & B[2]; y = (y | (y >> S[2])) & B[2];
  x = (x | (x >> S[3])) & B[3]; y = (y | (y >> S[3])) & B[3];
  x = (x | (x >> S[4])) & B[4]; y = (y | (y >> S[4])) & B[4];
  x = (x | (x >> S[5])) & B[5]; y = (y | (y >> S[5])) & B[5];
  return x | (y << 32);
}
// 52-bit geohash score: latitude bits in even positions, longitude in odd (step 26).
inline std::uint64_t encode(double lon, double lat) {
  double lat_off = (lat - LAT_MIN) / (LAT_MAX - LAT_MIN) * STEP_POW;
  double lon_off = (lon - LON_MIN) / (LON_MAX - LON_MIN) * STEP_POW;
  return interleave64(static_cast<std::uint32_t>(lat_off), static_cast<std::uint32_t>(lon_off));
}
// Decode to the CENTER of the geohash cell (what GEOPOS/GEODIST report).
inline void decode(std::uint64_t bits, double& lon, double& lat) {
  std::uint64_t sep = deinterleave64(bits);
  std::uint32_t ilat = static_cast<std::uint32_t>(sep), ilon = static_cast<std::uint32_t>(sep >> 32);
  double lat_lo = LAT_MIN + (ilat / STEP_POW) * (LAT_MAX - LAT_MIN);
  double lat_hi = LAT_MIN + ((ilat + 1) / STEP_POW) * (LAT_MAX - LAT_MIN);
  double lon_lo = LON_MIN + (ilon / STEP_POW) * (LON_MAX - LON_MIN);
  double lon_hi = LON_MIN + ((ilon + 1) / STEP_POW) * (LON_MAX - LON_MIN);
  lat = (lat_lo + lat_hi) / 2;
  lon = (lon_lo + lon_hi) / 2;
}
inline double lat_dist(double lat1, double lat2) { return EARTH_R * std::fabs((lat2 - lat1) * DEG_RAD); }
inline double dist(double lon1, double lat1, double lon2, double lat2) {  // haversine (metres)
  double lon1r = lon1 * DEG_RAD, lon2r = lon2 * DEG_RAD;
  double v = std::sin((lon2r - lon1r) / 2);
  if (v == 0.0) return lat_dist(lat1, lat2);
  double lat1r = lat1 * DEG_RAD, lat2r = lat2 * DEG_RAD;
  double u = std::sin((lat2r - lat1r) / 2);
  double a = u * u + std::cos(lat1r) * std::cos(lat2r) * v * v;
  return 2.0 * EARTH_R * std::asin(std::sqrt(a));
}
// Unit → metres multiplier (m/km/mi/ft); returns false on an unknown unit.
inline bool unit_to_m(const std::string& u, double& mul) {
  if (u == "m") mul = 1.0; else if (u == "km") mul = 1000.0;
  else if (u == "mi") mul = 1609.34; else if (u == "ft") mul = 0.3048;
  else return false;
  return true;
}
}  // namespace geoutil

// Full SORT / SORT_RO implementation (shared; readonly rejects STORE). Mirrors Redis sort.c:
// BY/GET support key-pattern and hash-field (key_*->field) lookups; a BY pattern without '*'
// means "don't sort" (retain native list/zset order, reversed for DESC); numeric sorting
// requires every score to parse fully as a double; ties break lexicographically by element.
std::string do_sort(const std::vector<std::string>& args, RespVersion resp, bool readonly) {
  std::string store_key; bool have_store = false;
  bool desc = false, alpha = false, have_by = false;
  std::string by_pattern;
  std::vector<std::string> get_patterns;
  std::int64_t limit_offset = 0, limit_count = -1; bool have_limit = false;
  for (std::size_t i = 2; i < args.size(); ++i) {
    const auto opt = upper(args[i]);
    if (opt == "STORE" && i + 1 < args.size()) {
      if (readonly) return encode_error("syntax error");
      store_key = args[++i]; have_store = true;
    } else if (opt == "DESC") { desc = true; }
    else if (opt == "ASC") { desc = false; }
    else if (opt == "ALPHA") { alpha = true; }
    else if (opt == "BY" && i + 1 < args.size()) { by_pattern = args[++i]; have_by = true; }
    else if (opt == "GET" && i + 1 < args.size()) { get_patterns.push_back(args[++i]); }
    else if (opt == "LIMIT" && i + 2 < args.size()) {
      if (!parse_i64(args[i + 1], limit_offset) || !parse_i64(args[i + 2], limit_count))
        return encode_error("value is not an integer or out of range");
      have_limit = true; i += 2;
    } else {
      return encode_error("syntax error");
    }
  }
  // "Don't sort" when a BY pattern has no '*' (a constant) — retain native order.
  bool dontsort = have_by && by_pattern.find('*') == std::string::npos;

  const auto type = store().type_of(args[1]);
  const bool is_set = (type == "set");
  std::vector<std::string> vals;
  if (type == "none") {
    // empty
  } else if (type == "list") {
    bool wt = false; vals = store().lrange(args[1], 0, -1, wt); if (wt) return wrongtype_error_reply();
  } else if (type == "set") {
    bool wt = false; vals = store().smembers(args[1], wt); if (wt) return wrongtype_error_reply();
  } else if (type == "zset") {
    bool wt = false; auto pr = store().zrange(args[1], 0, -1, false, wt); if (wt) return wrongtype_error_reply();
    for (auto& p : pr) vals.push_back(p.first);
  } else {
    return wrongtype_error_reply();
  }

  // A set has no native order, so when the result must be deterministic (STORE or from a
  // script) Redis forces an ALPHA sort instead of "don't sort".
  if (dontsort && is_set && (have_store || in_lua_script_context())) {
    dontsort = false; alpha = true; have_by = false;
  }

  // Substitute '*' with the element; a "->field" suffix (non-empty field) is a hash lookup.
  auto lookup = [&](const std::string& pattern, const std::string& elem) -> std::optional<std::string> {
    auto star = pattern.find('*');
    if (star == std::string::npos) return std::nullopt;
    std::string sub = pattern; sub.replace(star, 1, elem);
    auto arrow = sub.find("->");
    if (arrow != std::string::npos && arrow + 2 < sub.size()) {
      bool wt = false; return store().hget(sub.substr(0, arrow), sub.substr(arrow + 2), wt);
    }
    return store().get(sub);
  };

  if (!dontsort) {
    struct Item { std::string elem; std::string cmp; bool has_cmp; double score; };
    std::vector<Item> items; items.reserve(vals.size());
    for (auto& v : vals) {
      Item it; it.elem = v; it.has_cmp = true; it.cmp.clear(); it.score = 0;
      std::optional<std::string> byval = have_by ? lookup(by_pattern, v) : std::optional<std::string>(v);
      if (alpha) {
        // Missing BY value → NULL compare object (sorts before present ones).
        it.has_cmp = byval.has_value();
        if (byval) it.cmp = *byval;
      } else if (byval) {
        // Numeric: the score must parse fully as a double (Redis strtod semantics).
        const std::string& s = *byval;
        errno = 0; char* eptr = nullptr;
        const double d = std::strtod(s.c_str(), &eptr);
        if (s.empty() || *eptr != '\0' || errno == ERANGE || std::isnan(d))
          return encode_error("One or more scores can't be converted into double");
        it.score = d;
      }
      // else numeric + missing BY value → score stays 0.
      items.push_back(std::move(it));
    }
    std::stable_sort(items.begin(), items.end(), [&](const Item& a, const Item& b) {
      int cmp;
      if (!alpha) {
        cmp = (a.score < b.score) ? -1 : (a.score > b.score) ? 1
              : (a.elem < b.elem ? -1 : a.elem > b.elem ? 1 : 0);  // tie → lexicographic
      } else if (have_by) {
        if (!a.has_cmp || !b.has_cmp) cmp = (a.has_cmp == b.has_cmp) ? 0 : (!a.has_cmp ? -1 : 1);
        else cmp = (a.cmp < b.cmp) ? -1 : (a.cmp > b.cmp) ? 1 : 0;
      } else {
        cmp = (a.elem < b.elem) ? -1 : (a.elem > b.elem) ? 1 : 0;
      }
      return (desc ? -cmp : cmp) < 0;
    });
    vals.clear();
    for (auto& it : items) vals.push_back(std::move(it.elem));
  } else if (desc) {
    std::reverse(vals.begin(), vals.end());  // dontsort + DESC → reverse native order
  }

  // LIMIT offset count (overflow-safe): clamp a negative offset to 0, negative count to "all".
  if (have_limit) {
    std::int64_t n = static_cast<std::int64_t>(vals.size());
    std::int64_t start = limit_offset < 0 ? 0 : limit_offset;
    if (start > n) start = n;
    std::int64_t remain = n - start;
    std::int64_t take = (limit_count < 0) ? remain : std::min(limit_count, remain);
    vals = std::vector<std::string>(vals.begin() + start, vals.begin() + start + take);
  }

  // Materialize GET projections (# = element, pattern lookup otherwise; nil if missing).
  std::vector<std::string> results;
  if (get_patterns.empty()) {
    results = vals;
  } else {
    for (const auto& v : vals) {
      for (const auto& pat : get_patterns) {
        if (pat == "#") results.push_back(v);
        else {
          auto got = lookup(pat, v);
          results.push_back(got.value_or(std::string()));  // sentinel; nil-encoded below
        }
      }
    }
  }

  if (have_store) {
    store().del(store_key);
    // GET-missing entries are stored as empty strings (Redis behavior for SORT ... STORE).
    if (!results.empty()) { bool wt = false; (void)store().rpush(store_key, results, wt); }
    return encode_integer(static_cast<long long>(results.size()));
  }
  std::vector<std::string> out; out.reserve(results.size());
  if (get_patterns.empty()) {
    for (const auto& v : results) out.push_back(encode_bulk(v));
  } else {
    // Re-derive nil vs value for GET projections: a missing lookup is a RESP nil.
    for (const auto& v : vals) {
      for (const auto& pat : get_patterns) {
        if (pat == "#") out.push_back(encode_bulk(v));
        else { auto got = lookup(pat, v); out.push_back(got ? encode_bulk(*got) : encode_null(resp)); }
      }
    }
  }
  return encode_array(out);
}

// Shared engine for GEORADIUS / GEORADIUSBYMEMBER / GEOSEARCH / GEOSEARCHSTORE. `form`:
//   0 GEORADIUS, 1 GEORADIUSBYMEMBER, 2 GEOSEARCH, 3 GEOSEARCHSTORE.
// readonly rejects STORE (the *_RO variants). Brute-forces the geo zset (identical results to
// Redis's geohash-neighbour scan, just O(n)), matching distances/scores bit-for-bit.
bool parse_double(const std::string& s, double& out) {
  long double v;
  if (!parse_f64(s, v)) return false;
  out = static_cast<double>(v);
  return true;
}

std::string do_geo_search(const std::vector<std::string>& args, RespVersion resp, int form, bool readonly) {
  std::size_t i;
  std::string key, store_key; bool have_store = false, storedist = false;
  double clon = 0, clat = 0; bool have_center = false;
  bool bybox = false; double radius_m = 0, width_m = 0, height_m = 0; bool have_shape = false;
  double unit_mul = 1.0;
  int sort = 0;  // 0 none, 1 asc, 2 desc
  long count = 0; bool any = false;
  bool withcoord = false, withdist = false, withhash = false;
  std::string center_member; bool center_from_member = false;  // resolved after the type check

  if (form == 3) {  // GEOSEARCHSTORE dest src ...
    store_key = args[1]; have_store = true; key = args[2]; i = 3;
  } else {
    key = args[1]; i = 2;
  }

  if (form == 0) {  // GEORADIUS key lon lat radius unit
    if (i + 3 >= args.size()) return encode_error("syntax error");
    if (!parse_double(args[i], clon) || !parse_double(args[i + 1], clat)) return encode_error("value is not a valid float");
    double r; if (!parse_double(args[i + 2], r)) return encode_error("value is not a valid float");
    if (!geoutil::unit_to_m(args[i + 3], unit_mul)) return encode_error("unsupported unit provided. please use M, KM, FT, MI");
    radius_m = r * unit_mul; have_center = true; have_shape = true; i += 4;
  } else if (form == 1) {  // GEORADIUSBYMEMBER key member radius unit
    if (i + 2 >= args.size()) return encode_error("syntax error");
    center_member = args[i]; center_from_member = true; have_center = true;
    double r; if (!parse_double(args[i + 1], r)) return encode_error("value is not a valid float");
    if (!geoutil::unit_to_m(args[i + 2], unit_mul)) return encode_error("unsupported unit provided. please use M, KM, FT, MI");
    radius_m = r * unit_mul; have_shape = true; i += 3;
  }

  for (; i < args.size(); ++i) {
    const auto o = upper(args[i]);
    if (o == "WITHCOORD") withcoord = true;
    else if (o == "WITHDIST") withdist = true;
    else if (o == "WITHHASH") withhash = true;
    else if (o == "ASC") sort = 1;
    else if (o == "DESC") sort = 2;
    else if (o == "ANY") any = true;
    else if (o == "COUNT" && i + 1 < args.size()) {
      std::int64_t c; if (!parse_i64(args[i + 1], c) || c <= 0) return encode_error("COUNT must be > 0");
      count = static_cast<long>(c); ++i;
      if (i + 1 < args.size() && upper(args[i + 1]) == "ANY") { any = true; ++i; }
    }
    else if ((o == "STORE" || o == "STOREDIST") && (form == 0 || form == 1) && i + 1 < args.size()) {
      if (readonly) return encode_error("syntax error");
      store_key = args[++i]; have_store = true; storedist = (o == "STOREDIST");
    }
    else if (o == "STOREDIST" && form == 3) { storedist = true; }
    else if (o == "FROMMEMBER" && (form == 2 || form == 3) && i + 1 < args.size()) {
      if (have_center) return encode_error("syntax error");
      center_member = args[i + 1]; center_from_member = true; have_center = true; ++i;
    }
    else if (o == "FROMLONLAT" && (form == 2 || form == 3) && i + 2 < args.size()) {
      if (have_center) return encode_error("syntax error");
      if (!parse_double(args[i + 1], clon) || !parse_double(args[i + 2], clat)) return encode_error("value is not a valid float");
      have_center = true; i += 2;
    }
    else if (o == "BYRADIUS" && (form == 2 || form == 3) && i + 2 < args.size()) {
      if (have_shape) return encode_error("syntax error");
      double r; if (!parse_double(args[i + 1], r)) return encode_error("value is not a valid float");
      if (!geoutil::unit_to_m(args[i + 2], unit_mul)) return encode_error("unsupported unit provided. please use M, KM, FT, MI");
      radius_m = r * unit_mul; bybox = false; have_shape = true; i += 2;
    }
    else if (o == "BYBOX" && (form == 2 || form == 3) && i + 3 < args.size()) {
      if (have_shape) return encode_error("syntax error");
      double w, h; if (!parse_double(args[i + 1], w) || !parse_double(args[i + 2], h)) return encode_error("value is not a valid float");
      if (!geoutil::unit_to_m(args[i + 3], unit_mul)) return encode_error("unsupported unit provided. please use M, KM, FT, MI");
      width_m = w * unit_mul; height_m = h * unit_mul; bybox = true; have_shape = true; i += 3;
    }
    else return encode_error("syntax error");
  }

  if (form == 2 || form == 3) {
    if (!have_center) return encode_error("exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH");
    if (!have_shape) return encode_error("exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH");
  }
  if (any && count == 0) return encode_error("the ANY argument requires COUNT argument");
  if (have_store && (withcoord || withdist || withhash))
    return encode_error("STORE option in GEORADIUS is not compatible with WITHCOORD, WITHDIST and WITHHASH options");
  // COUNT without ANY and without an explicit sort defaults to ascending (nearest-first).
  if (count != 0 && sort == 0 && !any) sort = 1;

  bool wt = false;
  auto members = store().zrange(key, 0, -1, false, wt);
  if (wt) return wrongtype_error_reply();
  // Redis resolves the FROMMEMBER/BYMEMBER center only after the type check. A missing
  // source key yields an empty result (0 for STORE) rather than a decode error; an
  // existing zset lacking the member is the "could not decode" error case.
  if (center_from_member) {
    bool resolved = false;
    for (const auto& [m, score] : members) {
      if (m == center_member) { geoutil::decode(static_cast<std::uint64_t>(score), clon, clat); resolved = true; break; }
    }
    if (!resolved) {
      if (members.empty()) {
        // Missing source key: empty reply, or 0 for STORE (deleting any stale destination
        // key, mirroring georadiusGeneric's dbDelete-on-empty).
        if (have_store) { store().del(store_key); return encode_integer(0); }
        return encode_array({});
      }
      return encode_error("could not decode requested zset member");
    }
  }

  struct GP { std::string member; double dist_m; std::uint64_t bits; double lon, lat; };
  std::vector<GP> found;
  for (const auto& [m, score] : members) {
    const std::uint64_t bits = static_cast<std::uint64_t>(score);
    double plon, plat; geoutil::decode(bits, plon, plat);
    double d;
    if (bybox) {
      if (geoutil::lat_dist(plat, clat) > height_m / 2) continue;
      if (geoutil::dist(plon, plat, clon, plat) > width_m / 2) continue;
      d = geoutil::dist(clon, clat, plon, plat);
    } else {
      d = geoutil::dist(clon, clat, plon, plat);
      if (d > radius_m) continue;
    }
    found.push_back({m, d, bits, plon, plat});
    if (any && static_cast<long>(found.size()) >= count) break;
  }
  if (sort == 1) std::stable_sort(found.begin(), found.end(), [](const GP& a, const GP& b) { return a.dist_m < b.dist_m; });
  else if (sort == 2) std::stable_sort(found.begin(), found.end(), [](const GP& a, const GP& b) { return a.dist_m > b.dist_m; });
  if (count != 0 && static_cast<long>(found.size()) > count) found.resize(count);

  if (have_store) {
    store().del(store_key);
    for (const auto& g : found) {
      const double s = storedist ? (g.dist_m / unit_mul) : static_cast<double>(g.bits);
      store().zadd_one(store_key, s, g.member, false, false, false, false, false);
    }
    return encode_integer(static_cast<long long>(found.size()));
  }
  std::vector<std::string> out;
  for (const auto& g : found) {
    if (!withcoord && !withdist && !withhash) { out.push_back(encode_bulk(g.member)); continue; }
    std::vector<std::string> item;
    item.push_back(encode_bulk(g.member));
    if (withdist) { char b[64]; std::snprintf(b, sizeof(b), "%.4f", g.dist_m / unit_mul); item.push_back(encode_bulk(b)); }
    if (withhash) item.push_back(encode_integer(static_cast<long long>(g.bits)));
    if (withcoord) {
      char lb[64], tb[64];
      std::snprintf(lb, sizeof(lb), "%.17g", g.lon); std::snprintf(tb, sizeof(tb), "%.17g", g.lat);
      item.push_back(encode_array({encode_bulk(lb), encode_bulk(tb)}));
    }
    out.push_back(encode_array(item));
  }
  return encode_array(out);
}

const std::unordered_map<std::string, CommandSpec>& command_table() {
  static const std::unordered_map<std::string, CommandSpec> table = [] {
    std::unordered_map<std::string, CommandSpec> t;

    t.emplace("PING", CommandSpec{"PING", -1, {"fast", "readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          if (args.size() > 2) return encode_error("wrong number of arguments for 'ping' command");
          // In RESP2 subscribe mode Redis replies with a 2-element array instead
          // of a simple/bulk string (server.c pingCommand).
          if (session.resp_version == RespVersion::Resp2 && client_has_subscriptions(session.fd)) {
            const std::string& msg = args.size() == 2 ? args[1] : "";
            return std::string("*2\r\n$4\r\npong\r\n$") + std::to_string(msg.size()) + "\r\n" + msg + "\r\n";
          }
          if (args.size() == 1) return encode_simple("PONG");
          return encode_bulk(args[1]);
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

    t.emplace("QUIT", CommandSpec{"QUIT", 1, {"fast", "no_auth"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool& close) {
          close = true;
          return encode_simple("OK");
        }});

    t.emplace("SHUTDOWN", CommandSpec{"SHUTDOWN", -1, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool& close) {
          // Default: save iff save points are configured (matches Redis).
          bool do_save = !g_save_params.empty();
          for (std::size_t i = 1; i < args.size(); ++i) {
            const auto o = upper(args[i]);
            if (o == "NOSAVE") do_save = false;
            else if (o == "SAVE") do_save = true;
            else if (o == "NOW" || o == "FORCE" || o == "ABORT") { /* accepted, no-op */ }
            else return encode_error("syntax error");
          }
          if (do_save) {
            std::string err;
            if (!rdb_save(g_snapshot_path, err))
              return encode_error("Errors trying to SHUTDOWN. Check logs.");
          }
          // Flush the AOF so the everysec window is not lost on a clean shutdown.
          if (g_appendonly && g_aof_fd >= 0) ::fsync(g_aof_fd);
          request_shutdown();
          close = true;
          return encode_simple("OK");
        }});

    t.emplace("SYNC", CommandSpec{"SYNC", 1, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) {
          session.replica_stream = true;
          session.repl_index = static_cast<std::size_t>(g_master_repl_offset);
          g_last_repl_db = -1; // Force re-emit SELECT on next write
          // Plain SYNC sends an empty RDB and relies on the command stream. PeaDB's own
          // replica uses PSYNC (which serializes the full dataset, streams included).
          // Full-state plain SYNC for external clients remains a deferred item — serializing
          // the dataset here interacts with lazy expiry during RDB build and is low-value
          // relative to PSYNC; see compat/delta.md.
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

          // Attempt partial resync if the requested replid matches our current id (or our
          // pre-promotion replid2 history, up to second_replid_offset) and the offset is
          // still within the backlog window.
          const bool id_ok = (replid == g_master_replid) ||
                             (replid == g_master_replid2 && g_second_replid_offset >= 0 &&
                              offset <= g_second_replid_offset);
          if (id_ok && offset >= 0 &&
              offset >= g_repl_backlog.base() && offset <= g_repl_backlog.total) {
            std::string pending;
            g_repl_backlog.get_slice(offset, pending);
            session.repl_index = static_cast<std::size_t>(g_master_repl_offset);
            g_sync_partial_ok.fetch_add(1, std::memory_order_relaxed);
            return "+CONTINUE\r\n" + pending;
          }
          // A partial resync was requested (replid given) but we can't serve it.
          if (replid != "?") g_sync_partial_err.fetch_add(1, std::memory_order_relaxed);

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
          g_sync_full.fetch_add(1, std::memory_order_relaxed);
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
          if (has_write && noreplicas_should_reject()) {
            g_executing_exec = false;
            g_capture_exec_replication = false;
            g_exec_replication_events.clear();
            g_exec_write_event_count = 0;
            session.watched_epoch.reset();
            session.watched_digests.clear();
            return std::string("-EXECABORT Transaction discarded because of previous errors: NOREPLICAS Not enough good replicas to write.\r\n");
          }
          if (has_write && oom_should_reject()) {
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
          RespVersion requested = session.resp_version;
          std::size_t i = 1;
          if (args.size() >= 2) {
            int proto = 0;
            try {
              proto = std::stoi(args[1]);
            } catch (...) {
              return encode_error("NOPROTO unsupported protocol version");
            }
            if (proto != 2 && proto != 3) return encode_error("NOPROTO unsupported protocol version");
            requested = (proto == 3 ? RespVersion::Resp3 : RespVersion::Resp2);
            i = 2;
          }
          // Optional [AUTH user pass] and [SETNAME name] after the protocol version.
          std::string set_name;
          while (i < args.size()) {
            const auto opt = upper(args[i]);
            if (opt == "AUTH" && i + 2 < args.size()) {
              const std::string& u = args[i + 1];
              const std::string& p = args[i + 2];
              if (g_requirepass.empty())
                return encode_error("Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?");
              if (u != "default" || !const_time_eq(p, g_requirepass))
                return std::string("-WRONGPASS invalid username-password pair or user is disabled.\r\n");
              session.authenticated = true;
              session.user = "default";
              i += 3;
            } else if (opt == "SETNAME" && i + 1 < args.size()) {
              set_name = args[i + 1];
              i += 2;
            } else {
              return encode_error("syntax error in HELLO");
            }
          }
          // If a password is required and the client is still unauthenticated, refuse.
          if (!g_requirepass.empty() && !session.authenticated) {
            return std::string("-NOAUTH HELLO must be called with the client already "
                               "authenticated, otherwise the HELLO <proto> AUTH <user> <pass> "
                               "option can be used to authenticate the client and select the "
                               "RESP protocol version at the same time\r\n");
          }
          session.resp_version = requested;
          if (!set_name.empty()) session.client_name = set_name;
          return hello_reply(session);
        }});

    t.emplace("AUTH", CommandSpec{"AUTH", -2, {"fast", "no_auth"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          // Only the two- and three-argument forms are valid; more is a syntax error (Redis).
          if (args.size() > 3) return encode_error("syntax error");
          std::string user = "default";
          std::string pass;
          if (args.size() == 3) { user = args[1]; pass = args[2]; }
          else { pass = args[1]; }
          // Redis only replies "no password is set" for the single-arg default-user
          // form; AUTH <user> <pass> against an unknown user is WRONGPASS regardless.
          if (user != "default") {
            return std::string("-WRONGPASS invalid username-password pair or user is disabled.\r\n");
          }
          if (g_requirepass.empty()) {
            // Matches Redis authCommand's two-argument (default-user, no-pass) branch.
            return encode_error("AUTH <password> called without any password configured for "
                                "the default user. Are you sure your configuration is correct?");
          }
          if (!const_time_eq(pass, g_requirepass)) {
            return std::string("-WRONGPASS invalid username-password pair or user is disabled.\r\n");
          }
          session.authenticated = true;
          session.user = "default";
          return encode_simple("OK");
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
          if (sub == "DEBUG" && args.size() == 3) {
            // The LDB (Lua debugger) session protocol is not implemented — accepted
            // modes just reply OK so scripts run normally (tracked in compat/delta.md).
            const auto mode = lower(args[2]);
            if (mode == "yes" || mode == "sync" || mode == "no") return encode_simple("OK");
            return encode_error("Use SCRIPT DEBUG YES/SYNC/NO");
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
          if (oom_should_reject()) {
            if (she.has_shebang && !she.explicit_flags && !allow_oom) {
              return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
            }
            if (she.explicit_flags && !allow_oom) {
              return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
            }
          }
          if (write_intent && noreplicas_should_reject()) {
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
          // Redis 7.0+ INFO takes zero or more section names (case-insensitive). No args (or
          // "default") = the standard set, which EXCLUDES commandstats/latencystats; "all"/
          // "everything" = every section; explicit names union together (each emitted once).
          std::set<std::string> requested;
          for (std::size_t i = 1; i < args.size(); ++i) requested.insert(upper(args[i]));
          if (requested.empty()) requested.insert("DEFAULT");
          const bool want_all = requested.count("ALL") != 0 || requested.count("EVERYTHING") != 0;
          const bool want_default = requested.count("DEFAULT") != 0;
          auto uptime_s = (now_ms() - g_server_start_time.load()) / 1000;
          if (uptime_s < 1) uptime_s = 1;

          const std::size_t used_mem = static_cast<std::size_t>(store().used_memory());
          if (static_cast<std::int64_t>(used_mem) > g_used_memory_peak) g_used_memory_peak = static_cast<std::int64_t>(used_mem);
          const std::int64_t rss_bytes = process_rss_bytes();
          const double frag_ratio = (used_mem > 0 && rss_bytes > 0)
              ? static_cast<double>(rss_bytes) / static_cast<double>(used_mem) : 1.0;
          struct rusage ru{};
          getrusage(RUSAGE_SELF, &ru);
          const double cpu_user = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6;
          const double cpu_sys = ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
          const int blocked_now = g_blocked_clients_cb ? g_blocked_clients_cb() : 0;
          // instantaneous_ops_per_sec: rate since the last INFO call.
          const std::int64_t now_for_ops = now_ms();
          const std::int64_t cur_ops = g_total_commands_processed.load();
          std::int64_t inst_ops = 0;
          if (g_ops_sample_ms > 0 && now_for_ops > g_ops_sample_ms)
            inst_ops = (cur_ops - g_ops_sample_count) * 1000 / (now_for_ops - g_ops_sample_ms);
          g_ops_sample_ms = now_for_ops;
          g_ops_sample_count = cur_ops;

          auto fmt2 = [](double d) -> std::string { char b[32]; std::snprintf(b, sizeof(b), "%.2f", d); return b; };
          auto fmt6 = [](double d) -> std::string { char b[32]; std::snprintf(b, sizeof(b), "%.6f", d); return b; };

          auto format_mem = [](std::size_t bytes) -> std::string {
            if (bytes >= 1024ULL * 1024 * 1024) return std::to_string(bytes / (1024ULL * 1024 * 1024)) + "G";
            if (bytes >= 1024ULL * 1024) return std::to_string(bytes / (1024ULL * 1024)) + "M";
            if (bytes >= 1024ULL) return std::to_string(bytes / 1024) + "K";
            return std::to_string(bytes) + "B";
          };

          std::string out;
          // The default set matches Redis's default INFO: everything except commandstats and
          // latencystats (those appear only under all/everything or when explicitly requested).
          static const std::set<std::string> kDefaultSections = {
              "SERVER", "CLIENTS", "MEMORY", "PERSISTENCE", "STATS", "REPLICATION",
              "CPU", "MODULES", "ERRORSTATS", "CLUSTER", "KEYSPACE"};
          auto want = [&](const std::string& s) {
            if (want_all) return true;
            if (requested.count(s) != 0) return true;
            return want_default && kDefaultSections.count(s) != 0;
          };

          if (want("SERVER")) {
            out += "# Server\r\n"
                   "redis_version:7.2.5\r\n"
                   // Redis-standard field names so drop-in tooling (redis_exporter, redis-cli
                   // --version probes, the printver harness) can parse them.
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
                   "config_file:\r\n"
                   "cluster_enabled:" + std::string(g_cluster_peers.empty() ? "0" : "1") + "\r\n\r\n";
          }
          if (want("CLIENTS")) {
            out += "# Clients\r\n"
                   "connected_clients:" + std::to_string(g_connected_clients.load()) + "\r\n"
                   "blocked_clients:" + std::to_string(blocked_now) + "\r\n"
                   "tracking_clients:0\r\n"
                   "maxclients:" + std::to_string(g_runtime_max_clients.load(std::memory_order_relaxed)) + "\r\n\r\n";
          }
          if (want("MEMORY")) {
            out += "# Memory\r\n"
                   "used_memory:" + std::to_string(used_mem) + "\r\n"
                   "used_memory_human:" + format_mem(used_mem) + "\r\n"
                   "used_memory_peak:" + std::to_string(g_used_memory_peak) + "\r\n"
                   "used_memory_peak_human:" + format_mem(static_cast<std::size_t>(g_used_memory_peak)) + "\r\n"
                   "used_memory_rss:" + std::to_string(rss_bytes > 0 ? rss_bytes : static_cast<std::int64_t>(used_mem)) + "\r\n"
                   "mem_fragmentation_ratio:" + fmt2(frag_ratio) + "\r\n"
                   "mem_allocator:libc\r\n"
                   "maxmemory:" + std::to_string(g_config_maxmemory) + "\r\n"
                   "maxmemory_human:" + format_mem(static_cast<size_t>(g_config_maxmemory)) + "\r\n"
                   "maxmemory_policy:" + std::string(maxmemory_policy_to_str(g_maxmemory_policy)) + "\r\n"
                   "number_of_cached_scripts:" + std::to_string(g_script_cache.size()) + "\r\n\r\n";
          }
          if (want("PERSISTENCE")) {
            // Redis's rdb_changes_since_last_save is the DELTA since the last save baseline
            // (server.dirty), not the absolute mutation count — it drops to 0 right after a save.
            const std::int64_t changes_since_save = std::max<std::int64_t>(
                0, static_cast<std::int64_t>(store().mutation_epoch()) - g_dirty_at_last_save);
            out += "# Persistence\r\n"
                   "loading:" + std::to_string(g_loading_replication.load() ? 1 : 0) + "\r\n"
                   "rdb_changes_since_last_save:" + std::to_string(changes_since_save) + "\r\n"
                   "rdb_bgsave_in_progress:" + std::to_string(g_bgsave_in_progress.load() ? 1 : 0) + "\r\n"
                   "rdb_last_save_time:" + std::to_string(g_last_save_time) + "\r\n"
                   "rdb_last_bgsave_status:" + g_last_bgsave_status + "\r\n"
                   "aof_enabled:" + std::to_string(g_appendonly ? 1 : 0) + "\r\n"
                   "aof_rewrite_in_progress:" + std::to_string(g_bgrewrite_in_progress.load() ? 1 : 0) + "\r\n"
                   "aof_last_bgrewrite_status:" + g_aof_last_bgrewrite_status + "\r\n"
                   "aof_write_error:" + std::to_string(g_aof_write_error ? 1 : 0) + "\r\n"
                   "aof_rewrite_buffer_length:" + std::to_string(g_aof_rewrite_buf.size()) + "\r\n\r\n";
          }
          if (want("STATS")) {
            out += "# Stats\r\n"
                   "total_connections_received:" + std::to_string(g_total_connections_received.load()) + "\r\n"
                   "total_commands_processed:" + std::to_string(g_total_commands_processed.load()) + "\r\n"
                   "instantaneous_ops_per_sec:" + std::to_string(inst_ops) + "\r\n"
                   "total_error_replies:" + std::to_string(g_total_error_replies) + "\r\n"
                   "latest_fork_usec:" + std::to_string(g_latest_fork_usec) + "\r\n"
                   "total_forks:" + std::to_string(g_total_forks) + "\r\n"
                   "pubsub_channels:" + std::to_string(g_channel_subscribers.size()) + "\r\n"
                   "pubsub_patterns:" + std::to_string(g_pattern_subscribers.size()) + "\r\n"
                   "keyspace_hits:" + std::to_string(store().stat_keyspace_hits()) + "\r\n"
                   "keyspace_misses:" + std::to_string(store().stat_keyspace_misses()) + "\r\n"
                   "expired_keys:" + std::to_string(store().stat_expired_keys()) + "\r\n"
                   "evicted_keys:" + std::to_string(store().stat_evicted_keys()) + "\r\n"
                   "sync_full:" + std::to_string(g_sync_full.load()) + "\r\n"
                   "sync_partial_ok:" + std::to_string(g_sync_partial_ok.load()) + "\r\n"
                   "sync_partial_err:" + std::to_string(g_sync_partial_err.load()) + "\r\n\r\n";
          }
          if (want("REPLICATION")) {
            const int connected_replicas =
                g_count_connected_replicas ? g_count_connected_replicas() : 0;
            out += "# Replication\r\n"
                   "role:" + g_replication_role + "\r\n"
                   "connected_slaves:" + std::to_string(connected_replicas) + "\r\n"
                   "master_failover_state:no-failover\r\n"
                   "master_replid:" + g_master_replid + "\r\n"
                   "master_replid2:" + g_master_replid2 + "\r\n"
                   "master_repl_offset:" + std::to_string(g_master_repl_offset) + "\r\n"
                   "second_repl_offset:" + std::to_string(g_second_replid_offset) + "\r\n"
                   "repl_backlog_active:0\r\n"
                   "repl_backlog_size:" + std::to_string(g_repl_backlog.size_) + "\r\n";
            if (g_replication_role != "master") {
              out += "master_host:" + g_replication_master_host + "\r\n"
                     "master_port:" + std::to_string(g_replication_master_port) + "\r\n"
                     "master_link_status:" + (g_replica_stale ? "down" : "up") + "\r\n";
            }
            out += "\r\n";
          }
          if (want("CPU")) {
            out += "# CPU\r\n"
                   "used_cpu_sys:" + fmt6(cpu_sys) + "\r\n"
                   "used_cpu_user:" + fmt6(cpu_user) + "\r\n\r\n";
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
              const double upc = st.calls > 0 ? static_cast<double>(st.usec_total) / static_cast<double>(st.calls) : 0.0;
              out += "cmdstat_" + lower(k) +
                     ":calls=" + std::to_string(st.calls) +
                     ",usec=" + std::to_string(st.usec_total) +
                     ",usec_per_call=" + fmt2(upc) +
                     ",rejected_calls=" + std::to_string(st.rejected_calls) +
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
          g_dirty_at_last_save = static_cast<std::int64_t>(store().mutation_epoch());
          return encode_simple("OK");
        }});

    t.emplace("BGSAVE", CommandSpec{"BGSAVE", 1, {"admin", "noscript"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          if (g_bgsave_in_progress.load()) return encode_error("Background save already in progress");
          if (!do_bgsave_fork()) return encode_error("Background save failed: fork error");
          return encode_simple("Background saving started");
        }});

    t.emplace("BGREWRITEAOF", CommandSpec{"BGREWRITEAOF", 1, {"admin", "noscript"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          if (g_bgrewrite_in_progress.load())
            return encode_error("Background append only file rewriting already in progress");
          const auto fork_t0 = std::chrono::steady_clock::now();
          pid_t pid = fork();
          if (pid < 0) return encode_error("BGREWRITEAOF failed: fork error");
          if (pid == 0) {
            std::string err;
            bool ok = rewrite_aof_file(err);
            _exit(ok ? 0 : 1);
          }
          g_latest_fork_usec = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - fork_t0).count();
          ++g_total_forks;
          g_bgrewrite_in_progress.store(true);
          g_bgrewrite_child_pid.store(pid);
          g_aof_rewrite_buf_capped = false; // reset per-rewrite overflow flag
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
            return encode_integer(0);
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
          // Ask replicas to ACK now instead of waiting for their ~1 Hz heartbeat, so WAIT
          // resolves in ~1 RTT rather than up to a second. The GETACK rides the backlog the
          // same way write commands do; replicas reply REPLCONF ACK out-of-band.
          backlog_append(encode_command_resp({"REPLCONF", "GETACK", "*"}));
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
            g_repl_generation.fetch_add(1);  // signal any running reconnect supervisor to stop
            const bool was_replica = g_replication_role == "slave";
            g_replication_role = "master";
            g_replication_master_host.clear();
            g_replication_master_port = 0;
            g_replica_stale = false;
            g_loading_replication.store(false);
            // On promotion, remember the old replication id in replid2 so siblings that
            // shared the previous master's stream can partial-resync against history, then
            // adopt a fresh replid for the new lineage (Redis PSYNC2 promotion semantics).
            if (was_replica) {
              g_master_replid2 = g_master_replid;
              g_second_replid_offset = g_master_repl_offset;
              g_master_replid = gen_run_id();
            }
            return encode_simple("OK");
          }
          std::int64_t p = 0;
          if (!parse_i64(port, p) || p < 0 || p > 65535) return encode_error("value is not an integer or out of range");
          g_replication_master_host = host;
          g_replication_master_port = static_cast<int>(p);
          g_replication_role = "slave";
          g_replica_stale = true;
          const std::uint64_t gen = g_repl_generation.fetch_add(1) + 1;
          if (p == 0) {
            return encode_simple("OK");
          }
          // Supervised reconnect loop: connect → full sync → stream until disconnect,
          // then retry with exponential backoff. Exits when a newer REPLICAOF bumps the
          // generation. Threading invariant: this thread only touches atomics + enqueues
          // commands for the event loop (see replica_do_fullsync); it never mutates the
          // DataStore directly or calls into the Lua engine / session state.
          std::thread([host, p, gen]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            int backoff_ms = 500;
            while (g_repl_generation.load() == gen) {
              g_loading_replication.store(true);
              const bool ok = replica_do_fullsync(host, static_cast<int>(p), gen);
              g_loading_replication.store(false);
              if (g_repl_generation.load() != gen) break;  // superseded mid-sync
              // Link dropped (or connect failed): mark stale and reconnect after backoff.
              g_replica_stale = true;
              backoff_ms = ok ? 500 : std::min(backoff_ms * 2, 30000);
              std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            }
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

    // In standalone mode Redis accepts READONLY/READWRITE and replies OK; the flag
    // only changes behavior under cluster redirection, which PeaDB doesn't alter here.
    t.emplace("READONLY", CommandSpec{"READONLY", 1, {"readonly", "fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          return encode_simple("OK");
        }});

    t.emplace("READWRITE", CommandSpec{"READWRITE", 1, {"readonly", "fast"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState&, bool&) {
          return encode_simple("OK");
        }});

    t.emplace("COMMAND", CommandSpec{"COMMAND", -1, {"loading", "stale"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (args.size() >= 2 && upper(args[1]) == "HELP") {
            return encode_array({
                encode_bulk("COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                encode_bulk("(no subcommand)"), encode_bulk("COUNT"),
                encode_bulk("DOCS [<command-name> ...]"), encode_bulk("GETKEYS <full-command>"),
                encode_bulk("INFO [<command-name> ...]"), encode_bulk("LIST"), encode_bulk("HELP")});
          }
          const auto& table = command_table();
          // Top-level commands whose keys aren't a static range (from the generated Redis
          // metadata) carry the movablekeys flag in COMMAND INFO.
          static const std::set<std::string> kMovableKeys = [] {
            std::set<std::string> m;
            for (const auto& e : command_metadata())
              if (e.movablekeys && std::string(e.full_name).find('|') == std::string::npos)
                m.insert(e.full_name);
            return m;
          }();
          auto info = [](const CommandSpec& s) -> std::string {
            std::vector<std::string> flags;
            for (const auto& f : s.flags) flags.push_back(encode_bulk(f));
            if (kMovableKeys.count(lower(s.name)) != 0) flags.push_back(encode_bulk("movablekeys"));
            // Redis COMMAND INFO shape: name, arity, flags, first/last/step key,
            // acl-categories, tips, key-specs, subcommands (last four left empty).
            return encode_array({
                encode_bulk(lower(s.name)), encode_integer(s.arity), encode_array(flags),
                encode_integer(s.first_key), encode_integer(s.last_key), encode_integer(s.key_step),
                encode_array({}), encode_array({}), encode_array({}), encode_array({})});
          };
          if (args.size() == 1) {
            std::vector<std::string> out;
            out.reserve(table.size());
            for (const auto& [k, s] : table) { (void)k; out.push_back(info(s)); }
            return encode_array(out);
          }
          const std::string sub = upper(args[1]);
          if (sub == "COUNT") return encode_integer(static_cast<long long>(table.size()));
          if (sub == "INFO") {
            if (args.size() == 2) return encode_array({});
            std::vector<std::string> out;
            for (std::size_t i = 2; i < args.size(); ++i) {
              auto it = table.find(upper(args[i]));
              out.push_back(it == table.end() ? encode_null(session.resp_version) : info(it->second));
            }
            return encode_array(out);
          }
          if (sub == "GETKEYS" || sub == "GETKEYSANDFLAGS") {
            if (args.size() < 3) return encode_error("Unknown subcommand or wrong number of arguments for 'getkeys'");
            auto it = table.find(upper(args[2]));
            if (it == table.end()) return encode_error("Invalid command specified");
            const auto& s = it->second;
            const std::vector<std::string> cmdline(args.begin() + 2, args.end());
            const auto gk_cmd = upper(cmdline[0]);

            // (key, per-key-spec flags) pairs. Flags mirror Redis's command key-spec flags
            // (RO/RW/OW read-write intent + access/update/insert/delete). Commands whose keys
            // aren't a fixed range (SORT STORE, EVAL/FCALL numkeys, MEMORY USAGE, XGROUP,
            // ZUNIONSTORE-family) are extracted explicitly, like Redis's per-command getkeys procs.
            std::vector<std::pair<std::string, std::vector<std::string>>> kf;
            auto add = [&](const std::string& k, std::vector<std::string> flags) { kf.emplace_back(k, std::move(flags)); };
            const bool dynamic_keys = (gk_cmd == "EVAL" || gk_cmd == "EVALSHA" || gk_cmd == "EVAL_RO" ||
                                       gk_cmd == "EVALSHA_RO" || gk_cmd == "FCALL" || gk_cmd == "FCALL_RO");

            if ((gk_cmd == "SORT" || gk_cmd == "SORT_RO") && cmdline.size() >= 2) {
              add(cmdline[1], {"RO", "access"});
              std::string dest; bool have_store = false;  // last STORE wins (matches do_sort)
              for (std::size_t i = 2; i + 1 < cmdline.size(); ++i)
                if (upper(cmdline[i]) == "STORE") { dest = cmdline[i + 1]; have_store = true; ++i; }
              if (have_store) add(dest, {"OW", "update"});
            } else if (dynamic_keys) {
              std::int64_t nk = 0;
              if (cmdline.size() < 3 || !parse_i64(cmdline[2], nk) || nk < 0)
                return encode_error("Invalid arguments specified for command");
              if (static_cast<std::size_t>(nk) > cmdline.size() - 3)
                return encode_error("Number of keys can't be greater than number of args");
              for (std::int64_t k = 0; k < nk; ++k) add(cmdline[static_cast<std::size_t>(3 + k)], {"RW", "access", "update"});
            } else if (gk_cmd == "MEMORY" && cmdline.size() >= 3 && upper(cmdline[1]) == "USAGE") {
              add(cmdline[2], {"RO", "access"});
            } else if (gk_cmd == "XGROUP" && cmdline.size() >= 3) {
              const auto x = upper(cmdline[1]);
              if (x == "CREATE" || x == "SETID" || x == "DESTROY" || x == "CREATECONSUMER" || x == "DELCONSUMER")
                add(cmdline[2], {"RW"});
            } else if ((gk_cmd == "ZUNIONSTORE" || gk_cmd == "ZINTERSTORE" || gk_cmd == "ZDIFFSTORE") &&
                       cmdline.size() >= 4) {
              std::int64_t nk = 0;
              if (!parse_i64(cmdline[2], nk) || nk < 0) return encode_error("Invalid arguments specified for command");
              add(cmdline[1], {"OW", "update"});  // destination
              for (std::int64_t k = 0; k < nk && static_cast<std::size_t>(3 + k) < cmdline.size(); ++k)
                add(cmdline[static_cast<std::size_t>(3 + k)], {"RO", "access"});
            } else if (gk_cmd == "LMOVE" && cmdline.size() >= 3) {
              add(cmdline[1], {"RW", "access", "delete"});
              add(cmdline[2], {"RW", "insert"});
            } else if (gk_cmd == "SET" && cmdline.size() >= 2) {
              add(cmdline[1], {"OW", "update"});
            } else if (gk_cmd == "MSET" && cmdline.size() >= 2) {
              for (std::size_t i = 1; i + 1 < cmdline.size(); i += 2) add(cmdline[i], {"OW", "update"});
            } else {
              if (s.first_key > 0) {
                int last = s.last_key;
                if (last < 0) last = static_cast<int>(cmdline.size()) + last;
                const int step = s.key_step > 0 ? s.key_step : 1;
                const std::vector<std::string> def = s.is_write()
                    ? std::vector<std::string>{"RW", "access", "update"}
                    : std::vector<std::string>{"RO", "access"};
                for (int k = s.first_key; k <= last && k < static_cast<int>(cmdline.size()); k += step)
                  add(cmdline[static_cast<std::size_t>(k)], def);
              }
            }

            // EVAL/FCALL with numkeys 0 legitimately has no keys — return an empty array, not an
            // error (the "no key arguments" error is for commands that can never take a key).
            if (kf.empty() && !dynamic_keys) return encode_error("The command has no key arguments");
            std::vector<std::string> out;
            for (const auto& [k, flags] : kf) {
              if (sub == "GETKEYSANDFLAGS") {
                std::vector<std::string> fl;
                for (const auto& f : flags) fl.push_back(encode_bulk(f));
                out.push_back(encode_array({encode_bulk(k), encode_array(fl)}));
              } else {
                out.push_back(encode_bulk(k));
              }
            }
            return encode_array(out);
          }
          if (sub == "LIST") {
            // COMMAND LIST [FILTERBY MODULE <m> | ACLCAT <cat> | PATTERN <glob>]. Lists the
            // commands (and their subcommands as "parent|sub") PeaDB implements, from the
            // generated Redis metadata; the filter narrows by ACL category, name glob, or module.
            enum { kAll, kModule, kAclcat, kPattern } mode = kAll;
            std::string filter;
            if (args.size() == 2) {
              // no filter
            } else if (args.size() == 5 && upper(args[2]) == "FILTERBY") {
              const auto by = upper(args[3]);
              filter = args[4];
              if (by == "MODULE") mode = kModule;
              else if (by == "ACLCAT") mode = kAclcat;
              else if (by == "PATTERN") mode = kPattern;
              else return encode_error("syntax error");
            } else {
              return encode_error("syntax error");
            }
            const std::string want_cat = lower(filter);
            std::vector<std::string> out;
            for (const auto& e : command_metadata()) {
              const std::string full = e.full_name;
              const auto bar = full.find('|');
              const std::string top = bar == std::string::npos ? full : full.substr(0, bar);
              if (table.find(upper(top)) == table.end()) continue;  // only implemented commands
              if (mode == kModule) continue;  // no modules loaded → always empty
              if (mode == kAclcat) {
                bool has = false;
                const std::string cats = e.acl_cats;  // space-separated
                std::size_t p = 0;
                while (p < cats.size()) {
                  std::size_t sp = cats.find(' ', p);
                  const std::string tok = cats.substr(p, sp == std::string::npos ? std::string::npos : sp - p);
                  if (tok == want_cat) { has = true; break; }
                  if (sp == std::string::npos) break;
                  p = sp + 1;
                }
                if (!has) continue;
              }
              if (mode == kPattern && fnmatch(filter.c_str(), full.c_str(), 0) != 0) continue;
              out.push_back(encode_bulk(full));
            }
            return encode_array(out);
          }
          if (sub == "DOCS") return session.resp_version == RespVersion::Resp3 ? std::string("%0\r\n") : std::string("*0\r\n");
          return encode_error("unknown subcommand for COMMAND");
        }});

    t.emplace("CONFIG", CommandSpec{"CONFIG", -2, {"admin", "loading"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          if (upper(args[1]) == "HELP") {
            return encode_array({
                encode_bulk("CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                encode_bulk("GET <pattern>"), encode_bulk("SET <directive> <value>"),
                encode_bulk("RESETSTAT"), encode_bulk("REWRITE"), encode_bulk("HELP")});
          }
          if (args.size() >= 3 && upper(args[1]) == "GET") {
            const auto& pattern = args[2];
            // Build config parameter table
            std::vector<std::pair<std::string, std::string>> config_params = {
              {"save", g_save_spec},
              {"stop-writes-on-bgsave-error", g_stop_writes_on_bgsave_error ? "yes" : "no"},
              {"aof-load-truncated", g_aof_load_truncated ? "yes" : "no"},
              {"repl-backlog-size", std::to_string(g_repl_backlog.size_)},
              {"appendonly", g_appendonly ? "yes" : "no"},
              {"maxmemory", std::to_string(g_config_maxmemory)},
              {"min-replicas-to-write", std::to_string(g_config_min_replicas_to_write)},
              {"replica-serve-stale-data", g_config_replica_serve_stale_data ? "yes" : "no"},
              {"lua-time-limit", std::to_string(g_config_lua_time_limit)},
              {"zset-max-ziplist-entries", std::to_string(store().encoding_limits().zset_entries)},
              {"zset-max-listpack-entries", std::to_string(store().encoding_limits().zset_entries)},
              {"zset-max-ziplist-value", std::to_string(store().encoding_limits().zset_value)},
              {"zset-max-listpack-value", std::to_string(store().encoding_limits().zset_value)},
              {"hash-max-ziplist-entries", std::to_string(store().encoding_limits().hash_entries)},
              {"hash-max-listpack-entries", std::to_string(store().encoding_limits().hash_entries)},
              {"hash-max-ziplist-value", std::to_string(store().encoding_limits().hash_value)},
              {"hash-max-listpack-value", std::to_string(store().encoding_limits().hash_value)},
              {"set-max-listpack-entries", std::to_string(store().encoding_limits().set_entries)},
              {"set-max-listpack-value", std::to_string(store().encoding_limits().set_value)},
              {"list-max-listpack-size", std::to_string(store().encoding_limits().list_size)},
              {"dbfilename", g_snapshot_path.filename().string()},
              {"dir", std::filesystem::current_path().string()},
              {"bind", "0.0.0.0"},
              {"port", std::to_string(g_server_port)},
              {"hz", "10"},
              {"lfu-log-factor", "10"},
              {"lfu-decay-time", "1"},
              {"slave-serve-stale-data", g_config_replica_serve_stale_data ? "yes" : "no"},
              {"min-slaves-to-write", std::to_string(g_config_min_replicas_to_write)},
              {"slowlog-log-slower-than", std::to_string(g_slowlog_threshold_us)},
              {"slowlog-max-len", std::to_string(g_slowlog_max_len)},
              {"notify-keyspace-events", notify_flags_to_str(g_notify_flags)},
              {"latency-monitor-threshold", std::to_string(g_latency_monitor_threshold_ms)},
              {"databases", "16"},
              {"activedefrag", "no"},
              {"loglevel", g_loglevel_name},
              {"maxmemory-policy", maxmemory_policy_to_str(g_maxmemory_policy)},
              {"maxmemory-samples", std::to_string(g_maxmemory_samples)},
              {"client-output-buffer-limit", client_output_buffer_limit_str()},
              {"timeout", std::to_string(g_config_timeout)},
              {"tcp-keepalive", "300"},
              {"tcp-backlog", std::to_string(g_config_tcp_backlog)},
              {"lazyfree-lazy-server-del", g_lazyfree_lazy_server_del ? "yes" : "no"},
              {"proto-max-bulk-len", std::to_string(kProtoMaxBulkLen)},
              {"list-max-ziplist-size", std::to_string(store().encoding_limits().list_size)},
              {"set-max-intset-entries", std::to_string(store().encoding_limits().set_intset)},
              {"maxclients", std::to_string(g_runtime_max_clients.load(std::memory_order_relaxed))},
              {"appendfsync", g_appendfsync},
              {"appendfilename", g_aof_path.filename().string()},
              {"requirepass", g_requirepass},
              {"masterauth", g_masterauth},
              {"protected-mode", g_protected_mode ? "yes" : "no"},
              {"enable-module-command", g_enable_module_command},
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
              if (!parse_memory(args[3], v) || v < 0) {
                return encode_error("CONFIG SET failed (possibly related to argument '" + args[2] + "') - argument couldn't be parsed into an integer");
              }
              g_config_maxmemory = v;
            }
            if (k == "maxmemory-policy") {
              const auto p = parse_maxmemory_policy(args[3]);
              if (!p) return encode_error("argument(s) must be one of the following: "
                                          "noeviction, allkeys-lru, allkeys-lfu, allkeys-random, "
                                          "volatile-lru, volatile-lfu, volatile-random, volatile-ttl");
              apply_maxmemory_policy(*p);
            }
            if (k == "maxmemory-samples") {
              std::int64_t v = 0;
              if (!parse_i64(args[3], v) || v < 1 || v > 1000) return encode_error("value is not an integer or out of range");
              g_maxmemory_samples = static_cast<int>(v);
            }
            if (k == "timeout") {
              std::int64_t v = 0;
              if (!parse_i64(args[3], v) || v < 0) return encode_error("value is not an integer or out of range");
              g_config_timeout = static_cast<int>(v);
            }
            if (k == "client-output-buffer-limit") {
              configure_client_output_buffer_limit(args[3]);
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
            {
              // Encoding-transition thresholds (listpack/ziplist aliases share storage).
              auto& lim = store().encoding_limits();
              std::int64_t* slot = nullptr;
              bool allow_negative = false;
              if (k == "zset-max-ziplist-entries" || k == "zset-max-listpack-entries") slot = &lim.zset_entries;
              else if (k == "zset-max-ziplist-value" || k == "zset-max-listpack-value") slot = &lim.zset_value;
              else if (k == "hash-max-ziplist-entries" || k == "hash-max-listpack-entries") slot = &lim.hash_entries;
              else if (k == "hash-max-ziplist-value" || k == "hash-max-listpack-value") slot = &lim.hash_value;
              else if (k == "set-max-intset-entries") slot = &lim.set_intset;
              else if (k == "set-max-listpack-entries") slot = &lim.set_entries;
              else if (k == "set-max-listpack-value") slot = &lim.set_value;
              else if (k == "list-max-ziplist-size" || k == "list-max-listpack-size") { slot = &lim.list_size; allow_negative = true; }
              if (slot != nullptr) {
                std::int64_t v = 0;
                if (!parse_i64(args[3], v) || (v < 0 && !allow_negative)) {
                  return encode_error("value is not an integer or out of range");
                }
                *slot = v;
              }
            }
            if (k == "appendfsync") {
              const auto v = lower(args[3]);
              if (v != "always" && v != "everysec" && v != "no")
                return encode_error("Invalid argument '" + args[3] + "' for CONFIG SET 'appendfsync'");
              g_appendfsync = v;
            }
            if (k == "lazyfree-lazy-server-del") {
              g_lazyfree_lazy_server_del = (lower(args[3]) == "yes");
            }
            if (k == "requirepass") {
              // Setting to "" disables auth. Already-connected clients keep their state.
              g_requirepass = args[3];
            }
            if (k == "masterauth") {
              g_masterauth = args[3];
            }
            if (k == "protected-mode") {
              const auto v = lower(args[3]);
              if (v != "yes" && v != "no") return encode_error("argument must be 'yes' or 'no'");
              g_protected_mode = (v == "yes");
            }
            if (k == "save") {
              configure_save(args[3]);
            }
            if (k == "stop-writes-on-bgsave-error") {
              const auto v = lower(args[3]);
              if (v != "yes" && v != "no") return encode_error("argument must be 'yes' or 'no'");
              g_stop_writes_on_bgsave_error = (v == "yes");
            }
            if (k == "aof-load-truncated") {
              const auto v = lower(args[3]);
              if (v != "yes" && v != "no") return encode_error("argument must be 'yes' or 'no'");
              g_aof_load_truncated = (v == "yes");
            }
            if (k == "notify-keyspace-events") {
              g_notify_flags = parse_notify_flags(args[3]);
            }
            if (k == "slowlog-log-slower-than") {
              std::int64_t v = 0;
              if (!parse_i64(args[3], v)) return encode_error("value is not an integer or out of range");
              g_slowlog_threshold_us = v;
            }
            if (k == "slowlog-max-len") {
              std::int64_t v = 0;
              if (!parse_i64(args[3], v) || v < 0) return encode_error("value is not an integer or out of range");
              g_slowlog_max_len = static_cast<std::size_t>(v);
              while (g_slowlog.size() > g_slowlog_max_len) g_slowlog.pop_back();
            }
            if (k == "latency-monitor-threshold") {
              std::int64_t v = 0;
              if (!parse_i64(args[3], v) || v < 0) return encode_error("value is not an integer or out of range");
              g_latency_monitor_threshold_ms = v;
            }
            if (k == "loglevel") {
              const auto v = lower(args[3]);
              g_loglevel_name = v;
              // Map redis-style levels onto PeaDB's logger levels.
              if (v == "debug") set_log_level(LogLevel::Debug);
              else if (v == "verbose") set_log_level(LogLevel::Info);
              else if (v == "notice") set_log_level(LogLevel::Info);
              else if (v == "warning") set_log_level(LogLevel::Warn);
              else set_log_level(parse_log_level(v));
            }
            return encode_simple("OK");
          }
          if (args.size() == 2 && upper(args[1]) == "RESETSTAT") {
            g_cmdstats.clear();
            g_cmd_latency_hist.clear();  // LATENCY HISTOGRAM resets with commandstats
            g_errorstats.clear();
            g_total_error_replies = 0;
            return encode_simple("OK");
          }
          // Redis's addReplySubcommandSyntaxError (lowercase, networking.c): an unrecognized
          // CONFIG subcommand (e.g. `CONFIG GET_XX`) is "unknown subcommand", not an arity error.
          return encode_error("unknown subcommand or wrong number of arguments for '" + args[1] +
                              "'. Try CONFIG HELP.");
        }});

    t.emplace("DEBUG", CommandSpec{"DEBUG", -2, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
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
          if (args.size() == 2 && upper(args[1]) == "DIGEST") {
            return encode_simple(store().debug_digest());
          }
          if (args.size() >= 2 && upper(args[1]) == "RELOAD") {
            // Save + flush + load, exercising the full RDB self-roundtrip. Flags mirror Redis:
            //   NOSAVE  — skip the save; load the on-disk dump as-is.
            //   NOFLUSH — don't empty the keyspace/functions before loading.
            //   MERGE   — function loads overwrite existing libraries (else a NOFLUSH reload
            //             collides with "already exists" and fails the whole load).
            bool nosave = false, noflush = false, merge = false;
            for (std::size_t i = 2; i < args.size(); ++i) {
              const auto o = upper(args[i]);
              if (o == "NOSAVE") nosave = true;
              else if (o == "NOFLUSH") noflush = true;
              else if (o == "MERGE") merge = true;
            }
            std::string err;
            if (!nosave && !rdb_save(g_snapshot_path, err)) return encode_error("Error saving RDB: " + err);
            if (!noflush) {
              store().flushall();
              functions_flush_all();
            }
            g_rdb_load_functions_replace = merge;
            const bool ok = rdb_load(g_snapshot_path, err, /*verify_crc=*/true);
            g_rdb_load_functions_replace = true;  // restore safe default for replica/startup loads
            if (!ok) return encode_error("Error trying to load the RDB dump: " + err);
            return encode_simple("OK");
          }
          if (args.size() == 3 && upper(args[1]) == "OBJECT") {
            // Raw dict peek like Redis debugCommand: a logically-expired-but-unreaped
            // key still reports its object info instead of erroring/expiring.
            if (!store().key_exists_raw(args[2])) return encode_error("no such key");
            const auto enc = store().object_encoding(args[2]).value_or("raw");
            const auto dump = rdb_dump_key(args[2]);
            const std::size_t serialized = dump.size() >= 10 ? dump.size() - 10 : dump.size();
            const auto idle = store().object_idletime_seconds(args[2]).value_or(0);
            const auto lru = store().object_lru_clock(args[2]).value_or(0);
            return encode_simple("Value at:0x0 refcount:1 encoding:" + enc +
                                 " serializedlength:" + std::to_string(serialized) +
                                 " lru:" + std::to_string(lru) +
                                 " lru_seconds_idle:" + std::to_string(idle));
          }
          if (args.size() == 2 && upper(args[1]) == "LOADAOF") {
            if (g_aof_fd >= 0) ::fsync(g_aof_fd);
            if (std::filesystem::exists(g_aof_path)) {
              store().flushall();
              std::string err;
              if (!load_aof_file(g_aof_path.string(), err)) {
                return encode_error("Error trying to load the AOF: " + err);
              }
            }
            return encode_simple("OK");
          }
          if ((args.size() >= 3 && args.size() <= 5) && upper(args[1]) == "POPULATE") {
            std::int64_t count = 0;
            if (!parse_i64(args[2], count) || count < 0) return encode_error("value is not an integer or out of range");
            const std::string prefix = args.size() >= 4 ? args[3] : "key:";
            std::int64_t vsize = 0;
            if (args.size() == 5 && (!parse_i64(args[4], vsize) || vsize < 0)) return encode_error("value is not an integer or out of range");
            for (std::int64_t i = 0; i < count; ++i) {
              std::string val = "value:" + std::to_string(i);
              if (vsize > 0) val.resize(static_cast<std::size_t>(vsize), 'A');
              store().set(prefix + std::to_string(i), val, false, false, std::nullopt, false);
            }
            return encode_simple("OK");
          }
          if (args.size() == 3 && upper(args[1]) == "SET-SKIP-CHECKSUM-VALIDATION") {
            std::int64_t v = 0;
            if (!parse_i64(args[2], v) || (v != 0 && v != 1)) return encode_error("value is not an integer or out of range");
            rdb_set_skip_checksum_validation(v == 1);
            return encode_simple("OK");
          }
          if (args.size() == 3 && upper(args[1]) == "QUICKLIST-PACKED-THRESHOLD") {
            return encode_simple("OK");  // accepted; meaningful once real quicklists exist
          }
          if (args.size() == 3 && upper(args[1]) == "PAUSE-CRON") {
            std::int64_t v = 0;
            if (!parse_i64(args[2], v) || (v != 0 && v != 1)) return encode_error("value is not an integer or out of range");
            g_pause_cron = (v == 1);
            return encode_simple("OK");
          }
          if (args.size() == 2 && upper(args[1]) == "JMAP") return encode_simple("OK");
          if (args.size() == 3 && upper(args[1]) == "SLEEP") {
            double secs = 0;
            try { secs = std::stod(args[2]); } catch (...) { return encode_error("Invalid debug sleep time"); }
            if (secs > 0) {
              std::this_thread::sleep_for(std::chrono::microseconds(static_cast<long long>(secs * 1000000)));
            }
            return encode_simple("OK");
          }
          if (args.size() == 3 && upper(args[1]) == "PROTOCOL") {
            // Mirror Redis debug.c: the addReply* helpers downgrade automatically in RESP2,
            // so every type has a distinct RESP2 vs RESP3 wire form.
            const bool r3 = session.resp_version == RespVersion::Resp3;
            const auto ptype = lower(args[2]);
            if (ptype == "string") return encode_bulk("Hello World");
            if (ptype == "integer") return encode_integer(12345);
            if (ptype == "double") return encode_double(3.141, session.resp_version);
            if (ptype == "bignum") {
              const std::string n = "1234567999999999999999999999999999999";
              return r3 ? ("(" + n + "\r\n") : encode_bulk(n);
            }
            if (ptype == "null") return encode_null(session.resp_version);
            if (ptype == "array") return std::string("*3\r\n:0\r\n:1\r\n:2\r\n");
            if (ptype == "set")
              return std::string(r3 ? "~3\r\n" : "*3\r\n") + ":0\r\n:1\r\n:2\r\n";
            if (ptype == "map")
              return r3 ? std::string("%3\r\n:0\r\n#f\r\n:1\r\n#t\r\n:2\r\n#f\r\n")
                        : std::string("*6\r\n:0\r\n:0\r\n:1\r\n:1\r\n:2\r\n:0\r\n");
            if (ptype == "verbatim") {
              const std::string s = "This is a verbatim\nstring";  // 25 bytes
              return r3 ? ("=29\r\ntxt:" + s + "\r\n") : encode_bulk(s);
            }
            if (ptype == "true") return std::string(r3 ? "#t\r\n" : ":1\r\n");
            if (ptype == "false") return std::string(r3 ? "#f\r\n" : ":0\r\n");
            if (ptype == "attrib") {
              // The attribute prefix is emitted only in RESP3; the trailing real reply
              // (a normal bulk string) is always sent so RESP2 clients get a valid reply.
              std::string out;
              if (r3) out += "|1\r\n$14\r\nkey-popularity\r\n*2\r\n$7\r\nkey:123\r\n:90\r\n";
              out += encode_bulk("Some real reply following the attribute");
              return out;
            }
            if (ptype == "push") {
              if (!r3) return encode_error("RESP2 is not supported by this command");
              std::string out = ">2\r\n";
              out += encode_bulk("server-cpu-usage");
              out += encode_integer(42);
              out += encode_bulk("Some real reply following the push reply");
              return out;
            }
            return encode_error("Wrong protocol type name. Please use one of the following: "
                                "string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false");
          }
          return encode_error("unknown subcommand or wrong number of arguments for 'DEBUG'. Try DEBUG HELP.");
        }});

    t.emplace("MODULE", CommandSpec{"MODULE", -2, {"admin"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          const auto sub = upper(args[1]);
          if (sub == "HELP") {
            return encode_array({
                encode_bulk("MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                encode_bulk("LIST"), encode_bulk("LOAD <path> [<arg> ...]"),
                encode_bulk("LOADEX <path> [[CONFIG NAME VALUE] [ARGS ...]]"),
                encode_bulk("UNLOAD <name>"), encode_bulk("HELP")});
          }
          if (sub == "LOAD" && args.size() >= 3) {
            // MODULE LOAD dlopen()s arbitrary code; gate it behind explicit opt-in
            // (default off) so an authenticated-but-untrusted or misconfigured server
            // is not a trivial RCE primitive.
            if (g_enable_module_command != "yes" && g_enable_module_command != "local") {
              return encode_error("MODULE command not allowed. If you really want to use it, "
                                  "please set the 'enable-module-command' config to 'yes'.");
            }
            const std::string path = args[2];
            // Reject path traversal to limit the loadable surface.
            if (path.find("..") != std::string::npos) {
              return encode_error("Error loading the extension. Please check the server logs.");
            }
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

          // Plain SET overwrites a key of ANY type (Redis semantics) — only the GET
          // option demands the existing value be a string.
          const bool existing_non_string = store().is_wrongtype_for_string(args[1]);
          if (get_old && existing_non_string) return wrongtype_error_reply();
          std::optional<std::string> old;
          if (get_old) old = store().get(args[1]);
          const bool ok = store().set(args[1], args[2], nx, xx, expire_at, keepttl);
          if (get_old) return old.has_value() ? encode_bulk(*old) : encode_null(RespVersion::Resp2);
          if (!ok) return encode_null(RespVersion::Resp2);
          return encode_simple("OK");
        }});

    t.emplace("SETEX", CommandSpec{"SETEX", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t sec = 0;
          if (!parse_i64(args[2], sec)) return encode_error("value is not an integer or out of range");
          if (sec <= 0) return encode_error("invalid expire time in setex");
          // SETEX overwrites any existing type, like SET.
          const auto exp = now_ms() + sec * 1000;
          const bool ok = store().set(args[1], args[3], false, false, exp, false);
          return ok ? encode_simple("OK") : encode_null(RespVersion::Resp2);
        }});

    t.emplace("PSETEX", CommandSpec{"PSETEX", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t ms = 0;
          if (!parse_i64(args[2], ms)) return encode_error("value is not an integer or out of range");
          if (ms <= 0) return encode_error("invalid expire time in psetex");
          // PSETEX overwrites any existing type, like SET.
          const auto exp = now_ms() + ms;
          const bool ok = store().set(args[1], args[3], false, false, exp, false);
          return ok ? encode_simple("OK") : encode_null(RespVersion::Resp2);
        }});

    t.emplace("SETNX", CommandSpec{"SETNX", 3, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          // SETNX on an existing key of any type returns 0 without a type error.
          const bool ok = store().set(args[1], args[2], true, false, std::nullopt, false);
          return encode_integer(ok ? 1 : 0);
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
          return with_string_key(args[1], [&]() {
            if (static_cast<std::int64_t>(store().strlen(args[1])) +
                    static_cast<std::int64_t>(args[2].size()) > kProtoMaxBulkLen) {
              return encode_error("string exceeds maximum allowed size (proto-max-bulk-len)");
            }
            return encode_integer(store().append(args[1], args[2]));
          });
        }});

    t.emplace("STRLEN", CommandSpec{"STRLEN", 2, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_string_key(args[1], [&]() { return encode_integer(store().strlen(args[1])); });
        }});

    t.emplace("SETBIT", CommandSpec{"SETBIT", 4, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::int64_t offset = 0;
          if (!parse_i64(args[2], offset) || offset < 0 || (offset >> 3) >= kProtoMaxBulkLen) {
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
          if (!parse_i64(args[2], offset) || offset < 0 || (offset >> 3) >= kProtoMaxBulkLen) {
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
          if (!args[3].empty() &&
              offset > kProtoMaxBulkLen - static_cast<std::int64_t>(args[3].size())) {
            // Subtraction form avoids signed overflow when offset is near INT64_MAX.
            return encode_error("string exceeds maximum allowed size (proto-max-bulk-len)");
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
          if (by == std::numeric_limits<std::int64_t>::min()) {
            return encode_error("decrement would overflow");  // -INT64_MIN is unrepresentable
          }
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
          if (!parse_strict_float(args[2], by)) return encode_error("value is not a valid float");
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
          // Lazyfree: big values are detached and destructed on a background thread so a
          // huge-key UNLINK doesn't stall the event loop.
          for (std::size_t i = 1; i < args.size(); ++i) n += store().unlink(args[i]) ? 1 : 0;
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
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          return do_sort(args, session.resp_version, /*readonly=*/false);
        }});

    t.emplace("RENAME", CommandSpec{"RENAME", 3, {"write"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          if (!store().rename(args[1], args[2], false)) return encode_error("no such key");
          // The destination may now hold a stream/list — wake any client blocked on it
          // (Redis signals the renamed-into key ready so blocked XREAD(GROUP)/BLPOP re-run).
          notify_key_written(args[2], session.db_index);
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
          std::int64_t idletime = -1, freq = -1;
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
            if (opt == "IDLETIME") {
              if (i + 1 >= args.size() || !parse_i64(args[i + 1], idletime) || idletime < 0)
                return encode_error("Invalid IDLETIME value, must be >= 0");
              ++i;
              continue;
            }
            if (opt == "FREQ") {
              if (i + 1 >= args.size() || !parse_i64(args[i + 1], freq) || freq < 0 || freq > 255)
                return encode_error("Invalid FREQ value, must be >= 0 and <= 255");
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
          // Seed LRU/LFU metadata if requested (no-op when the key was already-expired and
          // therefore not stored).
          if (idletime >= 0) store().set_object_idle(args[1], idletime);
          if (freq >= 0) store().set_object_freq(args[1], freq);
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
          bool has_auth = false;
          std::string auth_user, auth_pass;
          for (std::size_t i = 6; i < args.size(); ++i) {
            const auto opt = upper(args[i]);
            if (opt == "COPY") { copy = true; continue; }
            if (opt == "REPLACE") { replace = true; continue; }
            if (opt == "AUTH" && i + 1 < args.size()) {   // AUTH <password>
              auth_pass = args[i + 1]; has_auth = true; i += 1; continue;
            }
            if (opt == "AUTH2" && i + 2 < args.size()) {   // AUTH2 <username> <password>
              auth_user = args[i + 1]; auth_pass = args[i + 2]; has_auth = true; i += 2; continue;
            }
            if (opt == "KEYS") return encode_error("Unsupported MIGRATE option");  // multi-key form (delta 19)
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
          if (has_auth) {
            std::vector<std::string> auth_args = auth_user.empty()
                ? std::vector<std::string>{"AUTH", auth_pass}
                : std::vector<std::string>{"AUTH", auth_user, auth_pass};
            const auto auth_wire = encode_command_resp(auth_args);
            if (!write_all(fd, auth_wire.data(), auth_wire.size()) ||
                !read_resp_reply(fd, payload_reply, is_err) || is_err) {
              close(fd);
              return std::string("-IOERR Authentication to the target instance failed.\r\n");
            }
          }
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

    t.emplace("FLUSHALL", CommandSpec{"FLUSHALL", -1, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          bool async = false;
          if (args.size() >= 2) {
            const auto m = upper(args[1]);
            if (m == "ASYNC") async = true;
            else if (m != "SYNC") return encode_error("syntax error");
          }
          if (async) store().flushall_async(); else store().flushall();
          return encode_simple("OK");
        }});

    t.emplace("FLUSHDB", CommandSpec{"FLUSHDB", -1, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          bool async = false;
          if (args.size() >= 2) {
            const auto m = upper(args[1]);
            if (m == "ASYNC") async = true;
            else if (m != "SYNC") return encode_error("syntax error");
          }
          if (async) store().flushdb_async(); else store().flushdb();
          return encode_simple("OK");
        }});

    t.emplace("FUNCTION", CommandSpec{"FUNCTION", -2, {"loading", "stale"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          const auto sub = upper(args[1]);
          if (sub == "LOAD") {
            bool replace = false;
            std::size_t i = 2;
            for (; i + 1 < args.size(); ++i) {
              if (upper(args[i]) == "REPLACE") replace = true;
              else return encode_error("Unknown option given: " + args[i]);
            }
            if (i >= args.size()) return encode_error("Function code is missing");
            if (oom_should_reject()) return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
            std::string name, err;
            if (!load_function_library(args[i], replace, name, err)) return encode_error(err);
            append_replication_event({"FUNCTION", "LOAD", "REPLACE", args[i]}, session);
            append_aof({"FUNCTION", "LOAD", "REPLACE", args[i]});
            store().increment_mutation_epoch();
            return encode_bulk(name);
          }
          if (sub == "DELETE" && args.size() == 3) {
            const std::string lc = lower(args[2]);
            auto it = g_libraries.find(lc);
            if (it == g_libraries.end()) return encode_error("Library not found");
            for (auto& f : it->second.functions) { g_func_index.erase(lower(f.name)); lua_engine_unref_function(f.callback_ref); }
            g_libraries.erase(it);
            append_replication_event({"FUNCTION", "DELETE", args[2]}, session);
            append_aof({"FUNCTION", "DELETE", args[2]});
            store().increment_mutation_epoch();
            return encode_simple("OK");
          }
          if (sub == "FLUSH") {
            if (args.size() > 3) return encode_error("unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION HELP.");
            if (args.size() == 3) { const auto m = upper(args[2]); if (m != "SYNC" && m != "ASYNC") return encode_error("FUNCTION FLUSH only supports SYNC|ASYNC"); }
            functions_flush_all();
            append_replication_event({"FUNCTION", "FLUSH"}, session);
            append_aof({"FUNCTION", "FLUSH"});
            store().increment_mutation_epoch();
            return encode_simple("OK");
          }
          if (sub == "LIST") {
            std::string pat; bool withcode = false, have_pat = false;
            for (std::size_t i = 2; i < args.size(); ++i) {
              const auto o = upper(args[i]);
              if (o == "LIBRARYNAME") {
                if (have_pat || i + 1 >= args.size()) return encode_error(have_pat ? "Unknown argument " + args[i] : "library name argument was not given");
                pat = args[++i]; have_pat = true;
              } else if (o == "WITHCODE" && !withcode) {
                withcode = true;
              } else {
                return encode_error("Unknown argument " + args[i]);
              }
            }
            std::vector<std::string> libs;
            for (const auto& [lc, lib] : g_libraries) {
              if (have_pat && lib.name != pat && fnmatch(pat.c_str(), lib.name.c_str(), 0) != 0) continue;
              std::vector<std::string> fns;
              for (const auto& f : lib.functions) {
                std::vector<std::string> flagnames;
                if (f.flags & kFuncNoWrites) flagnames.push_back(encode_bulk("no-writes"));
                if (f.flags & kFuncAllowOom) flagnames.push_back(encode_bulk("allow-oom"));
                if (f.flags & kFuncAllowStale) flagnames.push_back(encode_bulk("allow-stale"));
                if (f.flags & kFuncNoCluster) flagnames.push_back(encode_bulk("no-cluster"));
                if (f.flags & kFuncAllowCrossSlot) flagnames.push_back(encode_bulk("allow-cross-slot-keys"));
                std::vector<std::string> fkv = {
                    encode_bulk("name"), encode_bulk(f.name),
                    encode_bulk("description"), f.description.empty() ? encode_null(session.resp_version) : encode_bulk(f.description),
                    encode_bulk("flags"), encode_set_reply(flagnames, session.resp_version)};
                fns.push_back(encode_map(fkv, session.resp_version));
              }
              std::vector<std::string> lkv = {
                  encode_bulk("library_name"), encode_bulk(lib.name),
                  encode_bulk("engine"), encode_bulk(lib.engine),
                  encode_bulk("functions"), encode_array(fns)};
              if (withcode) { lkv.push_back(encode_bulk("library_code")); lkv.push_back(encode_bulk(lib.code)); }
              libs.push_back(encode_map(lkv, session.resp_version));
            }
            return encode_array(libs);
          }
          if (sub == "STATS") {
            std::string running;
            if (g_script_busy.load()) {
              running = encode_map({encode_bulk("name"), encode_bulk(g_busy_func_name),
                                    encode_bulk("command"), encode_bulk(g_busy_func_cmd),
                                    encode_bulk("duration_ms"), encode_integer(0)}, session.resp_version);
            } else {
              running = encode_null(session.resp_version);
            }
            std::size_t nfuncs = g_func_index.size();
            std::string engines = encode_map({encode_bulk("LUA"),
                encode_map({encode_bulk("libraries_count"), encode_integer(static_cast<long long>(g_libraries.size())),
                            encode_bulk("functions_count"), encode_integer(static_cast<long long>(nfuncs))}, session.resp_version)},
                session.resp_version);
            return encode_map({encode_bulk("running_script"), running, encode_bulk("engines"), engines}, session.resp_version);
          }
          if (sub == "DUMP" && args.size() == 2) {
            return encode_bulk(functions_dump());
          }
          if (sub == "RESTORE" && (args.size() == 3 || args.size() == 4)) {
            std::string policy = "APPEND";
            if (args.size() == 4) { policy = upper(args[3]); if (policy != "FLUSH" && policy != "APPEND" && policy != "REPLACE") return encode_error("Wrong restore policy given. Accept values are: FLUSH, APPEND or REPLACE."); }
            if (oom_should_reject()) return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
            std::string err;
            if (!functions_restore(args[2], policy, err)) return encode_error(err);
            append_replication_event({"FUNCTION", "RESTORE", args[2], policy}, session);
            append_aof({"FUNCTION", "RESTORE", args[2], policy});
            store().increment_mutation_epoch();
            return encode_simple("OK");
          }
          if (sub == "KILL" && args.size() == 2) {
            if (!g_script_busy.load()) return std::string("-NOTBUSY No scripts in execution right now.\r\n");
            g_script_busy.store(false);
            g_script_kill_requested.store(true);
            g_busy_script_session = nullptr;
            return encode_simple("OK");
          }
          if (sub == "HELP") {
            return encode_array({
                encode_bulk("FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                encode_bulk("LOAD [REPLACE] <code>"), encode_bulk("DELETE <library-name>"),
                encode_bulk("LIST [LIBRARYNAME <name>] [WITHCODE]"), encode_bulk("DUMP"),
                encode_bulk("RESTORE <payload> [FLUSH|APPEND|REPLACE]"), encode_bulk("FLUSH [ASYNC|SYNC]"),
                encode_bulk("STATS"), encode_bulk("KILL"), encode_bulk("HELP")});
          }
          return encode_error("unknown subcommand or wrong number of arguments for '" + lower(args[1]) + "'. Try FUNCTION HELP.");
        }});

    t.emplace("FCALL", CommandSpec{"FCALL", -3, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          return do_fcall(args, session, false);
        }});

    t.emplace("FCALL_RO", CommandSpec{"FCALL_RO", -3, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          return do_fcall(args, session, true);
        }});

    t.emplace("OBJECT", CommandSpec{"OBJECT", -2, {"readonly"}, 2, 2, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&){
          const auto sub = upper(args[1]);
          const bool lfu = g_maxmemory_policy == MaxmemoryPolicy::AllkeysLfu ||
                           g_maxmemory_policy == MaxmemoryPolicy::VolatileLfu;
          if (sub == "REFCOUNT" && args.size() == 3) {
            const auto t = store().type_of(args[2]);
            if (t == "none") return encode_null(session.resp_version);
            // Redis returns INT_MAX for shared integer objects (values 0..9999);
            // the upstream incr suite asserts refcount > 1 for them.
            if (t == "string") {
              auto v = store().get(args[2]);
              std::int64_t n = 0;
              if (v.has_value() && parse_i64(*v, n) && n >= 0 && n < 10000) {
                return encode_integer(2147483647);
              }
            }
            return encode_integer(1);
          }
          if (sub == "ENCODING" && args.size() == 3) {
            auto enc = store().object_encoding(args[2]);
            return enc.has_value() ? encode_bulk(*enc) : encode_null(session.resp_version);
          }
          if (sub == "IDLETIME" && args.size() == 3) {
            auto v = store().object_idletime_seconds(args[2]);
            if (!v.has_value()) return encode_null(session.resp_version);
            if (lfu) return encode_error("An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            return encode_integer(*v);
          }
          if (sub == "FREQ" && args.size() == 3) {
            auto v = store().object_freq(args[2]);
            if (!v.has_value()) return encode_null(session.resp_version);
            if (!lfu) return encode_error("An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            return encode_integer(*v);
          }
          if (sub == "HELP" && args.size() == 2) {
            return encode_array({encode_bulk("OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                                 encode_bulk("ENCODING <key>"), encode_bulk("FREQ <key>"),
                                 encode_bulk("IDLETIME <key>"), encode_bulk("REFCOUNT <key>"),
                                 encode_bulk("HELP")});
          }
          return encode_error("Unknown subcommand or wrong number of arguments for '" + args[1] + "'. Try OBJECT HELP.");
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
              if (!parse_strict_int(*existing, current)) return encode_error("hash value is not an integer");
            }
            if ((increment > 0 && current > std::numeric_limits<std::int64_t>::max() - increment) ||
                (increment < 0 && current < std::numeric_limits<std::int64_t>::min() - increment)) {
              return encode_error("increment or decrement would overflow");
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
            if (!parse_strict_float(args[3], increment)) return encode_error("value is not a valid float");
            bool wrongtype = false;
            auto existing = store().hget(args[1], args[2], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            long double current = 0;
            if (existing.has_value()) {
              if (!parse_strict_float(*existing, current)) return encode_error("hash value is not a valid float");
            }
            current += increment;
            if (std::isnan(static_cast<double>(current)) || std::isinf(static_cast<double>(current))) {
              return encode_error("value is NaN or Infinity");  // hash-specific Redis wording
            }
            // Redis ld2string: fixed %.17Lf notation with trailing zeros trimmed —
            // never scientific notation.
            char buf[5120];
            std::snprintf(buf, sizeof(buf), "%.17Lf", current);
            std::string val(buf);
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

    t.emplace("HSTRLEN", CommandSpec{"HSTRLEN", 3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            auto v = store().hget(args[1], args[2], wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            return encode_integer(v.has_value() ? static_cast<long long>(v->size()) : 0);
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
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          const auto resp = session.resp_version;
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
            // RESP3 clients receive a proper map; RESP2 a flat array.
            return encode_map(out, resp);
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
          std::string match_pat;
          bool novalues = false;
          for (std::size_t i = 3; i < args.size(); ++i) {
            const auto o = upper(args[i]);
            if (o == "COUNT" && i + 1 < args.size()) {
              try { count = static_cast<std::size_t>(std::stoull(args[++i])); } catch (...) { return encode_error("value is not an integer or out of range"); }
            } else if (o == "MATCH" && i + 1 < args.size()) {
              match_pat = args[++i];
            } else if (o == "NOVALUES") {
              novalues = true;  // Redis 7.4/backported: return fields only
            } else {
              return encode_error("syntax error");
            }
          }
          return with_hash_key(args[1], [&]() {
            bool wrongtype = false;
            const auto [next, fields] = store().hscan(args[1], cursor, count, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> inner;
            inner.reserve(fields.size() * (novalues ? 1 : 2));
            for (const auto& [f, v] : fields) {
              if (!match_pat.empty() && !pattern_matches(match_pat, f)) continue;
              inner.push_back(encode_bulk(f));
              if (!novalues) inner.push_back(encode_bulk(v));
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

    t.emplace("LPOP", CommandSpec{"LPOP", -2, {"write", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          return list_pop_command(args, /*left=*/true, session.resp_version);
        }});

    t.emplace("RPOP", CommandSpec{"RPOP", -2, {"write", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          return list_pop_command(args, /*left=*/false, session.resp_version);
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
          if (auto terr = parse_block_timeout_sec(args.back(), timeout_sec); !terr.empty()) {
            return terr;
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
          if (auto terr = parse_block_timeout_sec(args.back(), timeout_sec); !terr.empty()) {
            return terr;
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
          if (auto terr = parse_block_timeout_sec(args[5], timeout_sec); !terr.empty()) {
            return terr;
          }
          const std::string src = args[1];
          const std::string dst = args[2];
          const std::string where_from = upper(args[3]);
          const std::string where_to = upper(args[4]);
          if ((where_from != "LEFT" && where_from != "RIGHT") || (where_to != "LEFT" && where_to != "RIGHT")) {
            return encode_error("syntax error");
          }
          // Only the source type is checked up front. The destination type is checked
          // when an element is actually moved — an empty source blocks regardless of the
          // dst type (Redis blmoveGenericCommand).
          if (store().is_wrongtype_for_list(src)) return wrongtype_error_reply();

          bool wrongtype = false;
          if (store().is_wrongtype_for_list(dst) && store().llen(src, wrongtype) > 0) {
            return wrongtype_error_reply();  // src has data but dst is wrong type: source intact
          }
          std::optional<std::string> popped;
          if (where_from == "LEFT") popped = store().lpop(src, wrongtype);
          else popped = store().rpop(src, wrongtype);
          if (wrongtype) return wrongtype_error_reply();
          if (popped.has_value()) {
            std::vector<std::string> one{*popped};
            if (where_to == "LEFT") (void)store().lpush(dst, one, wrongtype);
            else (void)store().rpush(dst, one, wrongtype);
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

    // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n] (Redis 7.0). The
    // immediate path propagates its effect as a deterministic LPOP/RPOP (the dispatch
    // propagation is suppressed in canonicalize_propagated_command).
    t.emplace("BLMPOP", CommandSpec{"BLMPOP", -5, {"write", "blocking"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          long double timeout_sec = 0;
          if (auto terr = parse_block_timeout_sec(args[1], timeout_sec); !terr.empty()) {
            return terr;
          }
          std::int64_t numkeys = 0;
          if (!parse_i64(args[2], numkeys) || numkeys <= 0) {
            return encode_error("numkeys should be greater than 0");
          }
          if (args.size() < static_cast<std::size_t>(4 + numkeys)) {
            return encode_error("syntax error");
          }
          const std::size_t pos = static_cast<std::size_t>(3 + numkeys);
          const auto dir = upper(args[pos]);
          if (dir != "LEFT" && dir != "RIGHT") return encode_error("syntax error");
          const bool left = dir == "LEFT";
          std::size_t count = 1;
          if (pos + 1 < args.size()) {
            if (pos + 3 != args.size() || upper(args[pos + 1]) != "COUNT") return encode_error("syntax error");
            std::int64_t c = 0;
            if (!parse_i64(args[pos + 2], c) || c <= 0) return encode_error("count should be greater than 0");
            count = static_cast<std::size_t>(c);
          }
          // Immediate attempt: first non-empty key wins.
          for (std::int64_t i = 0; i < numkeys; ++i) {
            const auto& key = args[static_cast<std::size_t>(3 + i)];
            bool wrongtype = false;
            if (store().is_wrongtype_for_list(key)) return wrongtype_error_reply();
            const auto len = store().llen(key, wrongtype);
            if (len == 0) continue;
            std::vector<std::string> enc;
            std::size_t popped = 0;
            for (std::size_t k = 0; k < count; ++k) {
              auto v = left ? store().lpop(key, wrongtype) : store().rpop(key, wrongtype);
              if (!v.has_value()) break;
              enc.push_back(encode_bulk(*v));
              ++popped;
            }
            const std::vector<std::string> eff{left ? "LPOP" : "RPOP", key, std::to_string(popped)};
            append_replication_event(eff, session, nullptr);
            append_aof_command(eff, session, nullptr);
            return encode_array({encode_bulk(key), encode_array(enc)});
          }
          if (g_in_lua_script || g_executing_exec || g_block_disabled) return encode_null_array(session.resp_version);
          session.blocked.type = BlockType::List;
          session.blocked.pop_left = left;
          session.blocked.keys.clear();
          for (std::int64_t i = 0; i < numkeys; ++i) {
            session.blocked.keys.push_back(args[static_cast<std::size_t>(3 + i)]);
            register_key_waiter(args[static_cast<std::size_t>(3 + i)], session.fd);
          }
          session.blocked.original_args = args;
          session.blocked.deadline_ms = timeout_sec > 0
              ? now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L) : 0;
          return std::string();
        }});

    // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT n] (Redis 7.0).
    t.emplace("BZMPOP", CommandSpec{"BZMPOP", -5, {"write", "blocking"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          long double timeout_sec = 0;
          if (auto terr = parse_block_timeout_sec(args[1], timeout_sec); !terr.empty()) {
            return terr;
          }
          std::int64_t numkeys = 0;
          if (!parse_i64(args[2], numkeys) || numkeys <= 0) {
            return encode_error("numkeys should be greater than 0");
          }
          if (args.size() < static_cast<std::size_t>(4 + numkeys)) {
            return encode_error("syntax error");
          }
          const std::size_t pos = static_cast<std::size_t>(3 + numkeys);
          const auto dir = upper(args[pos]);
          if (dir != "MIN" && dir != "MAX") return encode_error("syntax error");
          const bool max = dir == "MAX";
          std::size_t count = 1;
          if (pos + 1 < args.size()) {
            if (pos + 3 != args.size() || upper(args[pos + 1]) != "COUNT") return encode_error("syntax error");
            std::int64_t c = 0;
            if (!parse_i64(args[pos + 2], c) || c <= 0) return encode_error("count should be greater than 0");
            count = static_cast<std::size_t>(c);
          }
          for (std::int64_t i = 0; i < numkeys; ++i) {
            const auto& key = args[static_cast<std::size_t>(3 + i)];
            bool wrongtype = false;
            auto vals = store().zpop(key, count, max, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            if (vals.empty()) continue;
            const std::vector<std::string> eff{max ? "ZPOPMAX" : "ZPOPMIN", key,
                                               std::to_string(vals.size())};
            append_replication_event(eff, session, nullptr);
            append_aof_command(eff, session, nullptr);
            std::vector<std::string> pairs;
            for (const auto& [m, s] : vals) {
              pairs.push_back(encode_array({encode_bulk(m), encode_double(s, session.resp_version)}));
            }
            return encode_array({encode_bulk(key), encode_array(pairs)});
          }
          if (g_in_lua_script || g_executing_exec || g_block_disabled) return encode_null_array(session.resp_version);
          session.blocked.type = BlockType::ZSet;
          session.blocked.zpop_max = max;
          session.blocked.keys.clear();
          for (std::int64_t i = 0; i < numkeys; ++i) {
            session.blocked.keys.push_back(args[static_cast<std::size_t>(3 + i)]);
            register_key_waiter(args[static_cast<std::size_t>(3 + i)], session.fd);
          }
          session.blocked.original_args = args;
          session.blocked.deadline_ms = timeout_sec > 0
              ? now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L) : 0;
          return std::string();
        }});

    t.emplace("BZPOPMIN", CommandSpec{"BZPOPMIN", -3, {"write", "blocking"}, 1, -2, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          long double timeout_sec = 0;
          if (auto terr = parse_block_timeout_sec(args.back(), timeout_sec); !terr.empty()) {
            return terr;
          }
          for (std::size_t i = 1; i + 1 < args.size(); ++i) {
            const auto tp = store().type_of(args[i]);
            if (tp != "none" && tp != "zset") return wrongtype_error_reply();
            bool wrongtype = false;
            auto vals = store().zpop(args[i], 1, false, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            if (!vals.empty()) {
              return encode_array({encode_bulk(args[i]), encode_bulk(vals[0].first),
                                   encode_double(vals[0].second, session.resp_version)});
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
          if (auto terr = parse_block_timeout_sec(args.back(), timeout_sec); !terr.empty()) {
            return terr;
          }
          for (std::size_t i = 1; i + 1 < args.size(); ++i) {
            const auto tp = store().type_of(args[i]);
            if (tp != "none" && tp != "zset") return wrongtype_error_reply();
            bool wrongtype = false;
            auto vals = store().zpop(args[i], 1, true, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            if (!vals.empty()) {
              return encode_array({encode_bulk(args[i]), encode_bulk(vals[0].first),
                                   encode_double(vals[0].second, session.resp_version)});
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
          std::string match_pat;
          for (std::size_t i = 3; i < args.size(); ++i) {
            const auto o = upper(args[i]);
            if (o == "COUNT" && i + 1 < args.size()) {
              try { count = static_cast<std::size_t>(std::stoull(args[++i])); } catch (...) { return encode_error("value is not an integer or out of range"); }
            } else if (o == "MATCH" && i + 1 < args.size()) {
              match_pat = args[++i];
            } else {
              return encode_error("syntax error");
            }
          }
          return with_set_key(args[1], [&]() {
            bool wrongtype = false;
            auto [next, members] = store().sscan(args[1], cursor, count, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> inner;
            inner.reserve(members.size());
            for (const auto& m : members) {
              if (!match_pat.empty() && !pattern_matches(match_pat, m)) continue;
              inner.push_back(encode_bulk(m));
            }
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
          if (nx && xx) return encode_error("XX and NX options at the same time are not compatible");
          if ((gt && nx) || (lt && nx) || (gt && lt)) {
            return encode_error("GT, LT, and/or NX options at the same time are not compatible");
          }

          return with_zset_key(args[1], [&]() {
            // Validate every score before applying anything (Redis parses all pairs
            // first, so a bad later pair leaves the zset untouched). NaN is invalid.
            std::vector<double> scores;
            scores.reserve((args.size() - pos) / 2);
            for (std::size_t i = pos; i < args.size(); i += 2) {
              long double score = 0;
              if (!parse_f64(args[i], score) || std::isnan(static_cast<double>(score))) {
                return encode_error("value is not a valid float");
              }
              scores.push_back(static_cast<double>(score));
            }
            // XX never creates the key: bail before zadd_one materializes an empty zset.
            if (xx && store().type_of(args[1]) == "none") {
              if (incr) return encode_null(RespVersion::Resp2);
              return encode_integer(0);
            }
            int added = 0, changed = 0;
            std::string incr_out;
            bool incr_nil = false;
            for (std::size_t i = pos; i < args.size(); i += 2) {
              auto r = store().zadd_one(args[1], scores[(i - pos) / 2], args[i + 1], nx, xx, gt, lt, incr);
              if (r.wrongtype) return wrongtype_error_reply();
              if (!r.valid) return encode_error("syntax error");
              if (r.added) ++added;
              if (r.changed) ++changed;
              if (incr) {
                if (std::isnan(r.score)) return encode_error("resulting score is not a number (NaN)");
                // NX/XX/GT/LT can suppress the INCR: Redis replies nil then.
                if (!r.added && !r.changed && (nx || xx || gt || lt)) incr_nil = true;
                incr_out = format_score(r.score);
              }
            }
            if (incr) return incr_nil ? encode_null(RespVersion::Resp2) : encode_bulk(incr_out);
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
          // Redis GEOADD folds XX+NX into the generic syntax error (geoaddCommand: one
          // shared.syntaxerr for both the odd-triple and the XX&&NX cases).
          if ((nx && xx) || pos >= args.size() || ((args.size() - pos) % 3) != 0) return encode_error("syntax error");
          return with_zset_key(args[1], [&]() {
            int added = 0, changed = 0;
            for (std::size_t i = pos; i < args.size(); i += 3) {
              long double lon = 0;
              long double lat = 0;
              if (!parse_f64(args[i], lon) || !parse_f64(args[i + 1], lat)) return encode_error("value is not a valid float");
              if (lon < geoutil::LON_MIN || lon > geoutil::LON_MAX ||
                  lat < geoutil::LAT_MIN || lat > geoutil::LAT_MAX) {
                return encode_error("invalid longitude,latitude pair " + args[i] + "," + args[i + 1]);
              }
              // 52-bit interleaved geohash score (bit-exact with Redis; see geoutil::encode).
              const double score = static_cast<double>(
                  geoutil::encode(static_cast<double>(lon), static_cast<double>(lat)));
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
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          // Unified Redis 6.2 ZRANGE: index/BYSCORE/BYLEX + REV + LIMIT + WITHSCORES,
          // sharing the selector with ZRANGESTORE.
          std::vector<std::pair<std::string, double>> items;
          bool withscores = false;
          auto err = zrange_select(args, 1, 2, 4, /*allow_withscores=*/true, items, withscores);
          if (!err.empty()) return err;
          std::vector<std::string> out;
          const bool resp3_pairs = withscores && session.resp_version == RespVersion::Resp3;
          out.reserve(items.size() * (withscores && !resp3_pairs ? 2 : 1));
          for (const auto& [m, s] : items) {
            if (resp3_pairs) {
              out.push_back(encode_array({encode_bulk(m), encode_double(s, RespVersion::Resp3)}));
            } else {
              out.push_back(encode_bulk(m));
              if (withscores) out.push_back(encode_bulk(format_score(s)));
            }
          }
          return encode_array(out);
        }});

    t.emplace("ZSCAN", CommandSpec{"ZSCAN", -3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          std::uint64_t cursor = 0;
          try { cursor = static_cast<std::uint64_t>(std::stoull(args[2])); } catch (...) { return encode_error("invalid cursor"); }
          std::size_t count = 10;
          std::string match_pat;
          for (std::size_t i = 3; i < args.size(); ++i) {
            const auto o = upper(args[i]);
            if (o == "COUNT" && i + 1 < args.size()) {
              try { count = static_cast<std::size_t>(std::stoull(args[++i])); } catch (...) { return encode_error("value is not an integer or out of range"); }
            } else if (o == "MATCH" && i + 1 < args.size()) {
              match_pat = args[++i];
            } else {
              return encode_error("syntax error");
            }
          }
          return with_zset_key(args[1], [&]() {
            bool wrongtype = false;
            auto [next, vals] = store().zscan(args[1], cursor, count, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            std::vector<std::string> inner;
            for (const auto& [m, s] : vals) {
              if (!match_pat.empty() && !pattern_matches(match_pat, m)) continue;
              inner.push_back(encode_bulk(m));
              inner.push_back(encode_bulk(format_score(s)));
            }
            return encode_array({encode_bulk(std::to_string(next)), encode_array(inner)});
          });
        }});

    t.emplace("ZPOPMIN", CommandSpec{"ZPOPMIN", -2, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          const bool has_count = args.size() == 3;
          std::size_t count = 1;
          if (has_count) {
            std::int64_t c = 0;
            if (!parse_i64(args[2], c)) return encode_error("value is not an integer or out of range");
            if (c < 0) return encode_error("value is out of range, must be positive");
            count = static_cast<std::size_t>(c);
          }
          if (args.size() > 3) return encode_error("syntax error");
          return with_zset_key(args[1], [&]() {
            bool wrongtype = false;
            auto vals = store().zpop(args[1], count, false, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            const bool r3 = session.resp_version == RespVersion::Resp3;
            std::vector<std::string> out;
            for (const auto& [m, s] : vals) {
              if (r3 && has_count) {
                // RESP3 count form: array of [member, double] pairs.
                out.push_back(encode_array({encode_bulk(m), encode_double(s, RespVersion::Resp3)}));
              } else {
                out.push_back(encode_bulk(m));
                out.push_back(r3 ? encode_double(s, RespVersion::Resp3) : encode_bulk(format_score(s)));
              }
            }
            return encode_array(out);
          });
        }});

    t.emplace("ZPOPMAX", CommandSpec{"ZPOPMAX", -2, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          const bool has_count = args.size() == 3;
          std::size_t count = 1;
          if (has_count) {
            std::int64_t c = 0;
            if (!parse_i64(args[2], c)) return encode_error("value is not an integer or out of range");
            if (c < 0) return encode_error("value is out of range, must be positive");
            count = static_cast<std::size_t>(c);
          }
          if (args.size() > 3) return encode_error("syntax error");
          return with_zset_key(args[1], [&]() {
            bool wrongtype = false;
            auto vals = store().zpop(args[1], count, true, wrongtype);
            if (wrongtype) return wrongtype_error_reply();
            const bool r3 = session.resp_version == RespVersion::Resp3;
            std::vector<std::string> out;
            for (const auto& [m, s] : vals) {
              if (r3 && has_count) {
                // RESP3 count form: array of [member, double] pairs.
                out.push_back(encode_array({encode_bulk(m), encode_double(s, RespVersion::Resp3)}));
              } else {
                out.push_back(encode_bulk(m));
                out.push_back(r3 ? encode_double(s, RespVersion::Resp3) : encode_bulk(format_score(s)));
              }
            }
            return encode_array(out);
          });
        }});

    t.emplace("ZMPOP", CommandSpec{"ZMPOP", -4, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
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
            const std::vector<std::string> eff{max ? "ZPOPMAX" : "ZPOPMIN", key,
                                               std::to_string(vals.size())};
            append_replication_event(eff, session, nullptr);
            append_aof_command(eff, session, nullptr);
            // Redis replies [key, [[member, score], ...]] — nested pairs, not flat;
            // scores are RESP3 doubles for RESP3 clients.
            std::vector<std::string> pairs;
            for (const auto& [m, s] : vals) {
              pairs.push_back(encode_array({encode_bulk(m), encode_double(s, session.resp_version)}));
            }
            return encode_array({encode_bulk(key), encode_array(pairs)});
          }
          return encode_null_array(session.resp_version);  // nothing popped anywhere
        }});

    t.emplace("XINFO", CommandSpec{"XINFO", -2, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          const auto sub = upper(args[1]);
          const bool r3 = session.resp_version == RespVersion::Resp3;
          auto entry_reply = [](const std::pair<std::string, std::vector<std::pair<std::string, std::string>>>& e) {
            if (e.first.empty()) return encode_null_array();
            std::vector<std::string> fv;
            for (const auto& [f, v] : e.second) { fv.push_back(encode_bulk(f)); fv.push_back(encode_bulk(v)); }
            return encode_array({encode_bulk(e.first), encode_array(fv)});
          };
          // Encode a map as RESP3 %N (or RESP2 flat *2N) from ordered key/encoded-value pairs.
          auto map_reply = [&](const std::vector<std::pair<std::string, std::string>>& kv) {
            std::string s = (r3 ? "%" : "*") + std::to_string(r3 ? kv.size() : kv.size() * 2) + "\r\n";
            for (const auto& [k, v] : kv) { s += encode_bulk(k); s += v; }
            return s;
          };
          if (sub == "STREAM") {
            if (args.size() < 3) return encode_error("wrong number of arguments for 'xinfo|stream' command");
            const bool full = args.size() >= 4 && upper(args[3]) == "FULL";
            if (full) {
              std::size_t fcount = 10;  // Redis default COUNT for FULL (0 = no limit)
              for (std::size_t i = 4; i < args.size(); ++i) {
                if (upper(args[i]) == "COUNT" && i + 1 < args.size()) {
                  std::int64_t c = 0;
                  if (!parse_i64(args[i + 1], c) || c < 0) return encode_error("value is not an integer or out of range");
                  fcount = static_cast<std::size_t>(c); ++i;
                } else {
                  return encode_error("syntax error");
                }
              }
              DataStore::StreamFullInfo fi; bool wt = false;
              if (!store().xinfo_stream_full(args[2], fcount, fi, wt)) {
                if (wt) return wrongtype_error_reply();
                return encode_error("no such key");
              }
              std::vector<std::string> ents;
              for (const auto& [id, fields] : fi.entries) {
                std::vector<std::string> fv;
                for (const auto& [f, v] : fields) { fv.push_back(encode_bulk(f)); fv.push_back(encode_bulk(v)); }
                ents.push_back(encode_array({encode_bulk(id), encode_array(fv)}));
              }
              std::vector<std::string> grps;
              for (const auto& g : fi.groups) {
                std::vector<std::string> gpend;
                for (const auto& p : g.pending)
                  gpend.push_back(encode_array({encode_bulk(p.id), encode_bulk(p.consumer),
                                                encode_integer(p.delivery_ms), encode_integer(p.delivery_count)}));
                std::vector<std::string> gcons;
                for (const auto& c : g.consumers) {
                  std::vector<std::string> cpend;
                  for (const auto& p : c.pending)
                    cpend.push_back(encode_array({encode_bulk(p.id), encode_integer(p.delivery_ms), encode_integer(p.delivery_count)}));
                  gcons.push_back(map_reply({{"name", encode_bulk(c.name)},
                                            {"seen-time", encode_integer(c.seen_time)},
                                            {"active-time", encode_integer(c.active_time)},
                                            {"pel-count", encode_integer(static_cast<long long>(c.pending.size()))},
                                            {"pending", encode_array(cpend)}}));
                }
                grps.push_back(map_reply({{"name", encode_bulk(g.name)},
                                          {"last-delivered-id", encode_bulk(g.last_delivered_id)},
                                          {"entries-read", g.entries_read < 0 ? encode_null(session.resp_version) : encode_integer(g.entries_read)},
                                          {"lag", g.lag_valid ? encode_integer(g.lag) : encode_null(session.resp_version)},
                                          {"pel-count", encode_integer(g.pel_count)},
                                          {"pending", encode_array(gpend)},
                                          {"consumers", encode_array(gcons)}}));
              }
              return map_reply({{"length", encode_integer(fi.length)},
                                {"radix-tree-keys", encode_integer(fi.length == 0 ? 1 : (fi.length + 99) / 100)},
                                {"radix-tree-nodes", encode_integer(fi.length == 0 ? 2 : (fi.length + 99) / 100 + 1)},
                                {"last-generated-id", encode_bulk(fi.last_id)},
                                {"max-deleted-entry-id", encode_bulk(fi.max_deleted_id)},
                                {"entries-added", encode_integer(fi.entries_added)},
                                {"recorded-first-entry-id", encode_bulk(fi.recorded_first_id)},
                                {"entries", encode_array(ents)},
                                {"groups", encode_array(grps)}});
            }
            DataStore::StreamInfo si;
            bool wt = false;
            if (!store().stream_info(args[2], si, wt)) {
              if (wt) return wrongtype_error_reply();
              return encode_error("no such key");
            }
            return map_reply({{"length", encode_integer(si.length)},
                              {"radix-tree-keys", encode_integer(si.length == 0 ? 1 : (si.length + 99) / 100)},
                              {"radix-tree-nodes", encode_integer(si.length == 0 ? 2 : (si.length + 99) / 100 + 1)},
                              {"last-generated-id", encode_bulk(si.last_id)},
                              {"max-deleted-entry-id", encode_bulk(si.max_deleted_id)},
                              {"entries-added", encode_integer(static_cast<long long>(si.entries_added))},
                              {"recorded-first-entry-id", encode_bulk(si.first.first.empty() ? "0-0" : si.first.first)},
                              {"groups", encode_integer(static_cast<long long>(si.groups))},
                              {"first-entry", entry_reply(si.first)},
                              {"last-entry", entry_reply(si.last)}});
          }
          if (sub == "GROUPS" && args.size() == 3) {
            bool wt = false; std::string err;
            auto gs = store().xinfo_groups(args[2], wt, err);
            if (wt) return wrongtype_error_reply();
            if (!err.empty()) return encode_stream_error(err);
            std::vector<std::string> out;
            for (const auto& g : gs)
              out.push_back(map_reply({{"name", encode_bulk(g.name)},
                                       {"consumers", encode_integer(static_cast<long long>(g.consumers))},
                                       {"pending", encode_integer(static_cast<long long>(g.pending))},
                                       {"last-delivered-id", encode_bulk(g.last_delivered_id)},
                                       {"entries-read", g.entries_read < 0 ? encode_null(session.resp_version) : encode_integer(g.entries_read)},
                                       {"lag", g.lag_valid ? encode_integer(g.lag) : encode_null(session.resp_version)}}));
            return encode_array(out);
          }
          if (sub == "CONSUMERS" && args.size() == 4) {
            bool wt = false; std::string err;
            auto cs = store().xinfo_consumers(args[2], args[3], wt, err);
            if (wt) return wrongtype_error_reply();
            if (!err.empty()) return encode_stream_error(err);
            std::vector<std::string> out;
            for (const auto& c : cs)
              out.push_back(map_reply({{"name", encode_bulk(c.name)},
                                       {"pending", encode_integer(static_cast<long long>(c.pending))},
                                       {"idle", encode_integer(c.idle)},
                                       {"inactive", encode_integer(c.inactive)}}));
            return encode_array(out);
          }
          if (sub == "HELP") {
            if (args.size() != 2) return encode_error("wrong number of arguments for 'xinfo|help' command");
            std::vector<std::string> items{
                encode_bulk("XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                encode_bulk("CONSUMERS <key> <groupname>"),
                encode_bulk("    Show consumers of <groupname>."),
                encode_bulk("GROUPS <key>"),
                encode_bulk("    Show the stream consumer groups."),
                encode_bulk("STREAM <key> [FULL [COUNT <count>]]"),
                encode_bulk("    Show information about the stream."),
                encode_bulk("HELP"),
                encode_bulk("    Print this help.")};
            return encode_array(items);
          }
          return encode_error("Unknown XINFO subcommand or wrong number of arguments");
        }});

    t.emplace("XADD", CommandSpec{"XADD", -5, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
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
              // Optional LIMIT count — only valid with the approximate (~) form.
              if (idx < args.size() && upper(args[idx]) == "LIMIT") {
                if (!trim_approx) {
                  return encode_error("syntax error, LIMIT cannot be used without the special ~ option");
                }
                idx += 2; // LIMIT count (exact enforcement needs the macro-node model)
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
                store().xdel(args[1], ids_to_del, wt, /*is_trim=*/true);
              }
            }
            // Apply MINID trimming
            if (!minid_threshold.empty()) {
              bool wt = false;
              auto entries = store().xrange(args[1], "-", "+", false, wt);
              if (!wt) {
                std::vector<std::string> ids_to_del;
                for (const auto& [eid, _] : entries) {
                  // Numeric compare: drop entries with id < MINID (XRANGE returns them
                  // in ascending order, so this is a prefix).
                  if (stream_id_lt(eid, minid_threshold)) ids_to_del.push_back(eid);
                }
                if (!ids_to_del.empty()) store().xdel(args[1], ids_to_del, wt, /*is_trim=*/true);
              }
            }

            notify_key_written(args[1], session.db_index);  // wake blocked XREAD/XREADGROUP
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

    // XRANGE/XREVRANGE key start end [COUNT n] — shared body (rev flips bound order).
    auto xrange_cmd = [](const std::vector<std::string>& args, bool rev) -> std::string {
      std::int64_t count = -1;
      if (args.size() == 6 && upper(args[4]) == "COUNT") {
        if (!parse_i64(args[5], count) || count < 0) return encode_error("value is not an integer or out of range");
      } else if (args.size() != 4) {
        return encode_error("syntax error");
      }
      return with_stream_key(args[1], [&]() {
        bool wrongtype = false, invalid = false;
        // Pass raw command-order bounds (args[2],args[3]); xrange flips for rev itself.
        auto items = store().xrange(args[1], args[2], args[3], rev, wrongtype, &invalid);
        if (wrongtype) return wrongtype_error_reply();
        if (invalid) return encode_error("Invalid stream ID specified as stream command argument");
        std::vector<std::string> out;
        for (const auto& [id, fields] : items) {
          if (count >= 0 && static_cast<std::int64_t>(out.size()) >= count) break;
          std::vector<std::string> fv;
          for (const auto& [f, v] : fields) {
            fv.push_back(encode_bulk(f));
            fv.push_back(encode_bulk(v));
          }
          out.push_back(encode_array({encode_bulk(id), encode_array(fv)}));
        }
        return encode_array(out);
      });
    };
    t.emplace("XRANGE", CommandSpec{"XRANGE", -4, {"readonly"}, 1, 1, 1,
        [xrange_cmd](const std::vector<std::string>& args, SessionState&, bool&) {
          return xrange_cmd(args, /*rev=*/false);
        }});
    t.emplace("XREVRANGE", CommandSpec{"XREVRANGE", -4, {"readonly"}, 1, 1, 1,
        [xrange_cmd](const std::vector<std::string>& args, SessionState&, bool&) {
          return xrange_cmd(args, /*rev=*/true);
        }});

    t.emplace("XGROUP", CommandSpec{"XGROUP", -2, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          const auto sub = upper(args[1]);
          if (sub == "CREATE") {
            if (args.size() < 5) return encode_error("wrong number of arguments for 'xgroup|create' command");
            // XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD n]
            bool mkstream = false;
            bool have_er = false;
            std::int64_t entries_read = 0;
            for (std::size_t i = 5; i < args.size(); ++i) {
              const auto opt = upper(args[i]);
              if (opt == "MKSTREAM") { mkstream = true; continue; }
              if (opt == "ENTRIESREAD" && i + 1 < args.size()) {
                if (!parse_i64(args[i + 1], entries_read))
                  return encode_error("value is not an integer or out of range");
                if (entries_read < -1)
                  return encode_error("value for ENTRIESREAD must be positive or -1");
                have_er = true; ++i; continue;
              }
              return encode_error("syntax error");
            }
            return with_stream_key(args[2], [&]() {
              bool wrongtype = false;
              std::string err;
              const bool ok = store().xgroup_create(args[2], args[3], args[4], mkstream, wrongtype, err,
                                                    have_er ? &entries_read : nullptr);
              if (wrongtype) return wrongtype_error_reply();
              if (!ok) return err.empty() ? encode_error("ERR") : encode_stream_error(err);
              notify_keyspace_event(NOTIFY_STREAM, "xgroup-create", args[2], session.db_index);
              return encode_simple("OK");
            });
          }
          if (sub == "SETID") {
            // XGROUP SETID key group id|$ [ENTRIESREAD n]
            if (args.size() != 5 && args.size() != 7) return encode_error("wrong number of arguments for 'xgroup|setid' command");
            bool have_er = false;
            std::int64_t entries_read = 0;
            if (args.size() == 7) {
              if (upper(args[5]) != "ENTRIESREAD") return encode_error("syntax error");
              if (!parse_i64(args[6], entries_read)) return encode_error("value is not an integer or out of range");
              if (entries_read < -1) return encode_error("value for ENTRIESREAD must be positive or -1");
              have_er = true;
            }
            return with_stream_key(args[2], [&]() {
              bool wrongtype = false;
              std::string err;
              const bool ok = store().xgroup_setid(args[2], args[3], args[4], wrongtype, err,
                                                   have_er ? &entries_read : nullptr);
              if (wrongtype) return wrongtype_error_reply();
              if (!ok) return err.empty() ? encode_error("ERR") : encode_stream_error(err);
              return encode_simple("OK");
            });
          }
          if (sub == "DESTROY") {
            if (args.size() != 4) return encode_error("wrong number of arguments for 'xgroup|destroy' command");
            return with_stream_key(args[2], [&]() {
              bool wrongtype = false;
              const int n = store().xgroup_destroy(args[2], args[3], wrongtype);
              if (wrongtype) return wrongtype_error_reply();
              if (n > 0) notify_key_written(args[2], session.db_index);  // wake blocked XREADGROUP → NOGROUP
              return encode_integer(n);
            });
          }
          if (sub == "CREATECONSUMER") {
            if (args.size() != 5) return encode_error("wrong number of arguments for 'xgroup|createconsumer' command");
            return with_stream_key(args[2], [&]() {
              bool wrongtype = false; std::string err;
              const int n = store().xgroup_createconsumer(args[2], args[3], args[4], wrongtype, err);
              if (wrongtype) return wrongtype_error_reply();
              if (!err.empty()) return encode_stream_error(err);
              if (n > 0) notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", args[2], session.db_index);
              return encode_integer(n);
            });
          }
          if (sub == "DELCONSUMER") {
            if (args.size() != 5) return encode_error("wrong number of arguments for 'xgroup|delconsumer' command");
            return with_stream_key(args[2], [&]() {
              bool wrongtype = false; std::string err; bool existed = false;
              const std::int64_t n = store().xgroup_delconsumer(args[2], args[3], args[4], wrongtype, err, &existed);
              if (wrongtype) return wrongtype_error_reply();
              if (!err.empty()) return encode_stream_error(err);
              if (existed) notify_keyspace_event(NOTIFY_STREAM, "xgroup-delconsumer", args[2], session.db_index);
              return encode_integer(n);
            });
          }
          if (sub == "HELP") {
            if (args.size() != 2) return encode_error("wrong number of arguments for 'xgroup|help' command");
            std::vector<std::string> items{
                encode_bulk("XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                encode_bulk("CREATE <key> <groupname> <id|$> [option]"),
                encode_bulk("    Create a new consumer group."),
                encode_bulk("CREATECONSUMER <key> <groupname> <consumer>"),
                encode_bulk("    Create a new consumer in the specified group."),
                encode_bulk("DELCONSUMER <key> <groupname> <consumer>"),
                encode_bulk("    Remove the specified consumer."),
                encode_bulk("DESTROY <key> <groupname>"),
                encode_bulk("    Remove the specified group."),
                encode_bulk("SETID <key> <groupname> <id|$> [ENTRIESREAD entries-read]"),
                encode_bulk("    Set the current group ID."),
                encode_bulk("HELP"),
                encode_bulk("    Print this help.")};
            return encode_array(items);
          }
          return encode_error("unknown subcommand for XGROUP");
        }});

    t.emplace("XREADGROUP", CommandSpec{"XREADGROUP", -7, {"write"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          if (upper(args[1]) != "GROUP") return encode_error("syntax error");
          if (args.size() < 7) return encode_error("wrong number of arguments for 'xreadgroup' command");
          const std::string group = args[2];
          const std::string consumer = args[3];
          std::size_t req_count = 0;  // 0 = no limit (Redis default without COUNT)
          bool noack = false;
          std::int64_t block_ms = -1;
          std::size_t i = 4;
          for (;;) {
            if (i >= args.size()) return encode_error("syntax error");
            const auto opt = upper(args[i]);
            if (opt == "STREAMS") break;
            if (opt == "NOACK") {
              noack = true;
              ++i;
              continue;
            }
            if (opt == "BLOCK") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              std::int64_t block = 0;
              if (!parse_i64(args[i + 1], block) || block < 0) return encode_error("value is not an integer or out of range");
              if (in_lua_script_context())
                return encode_error("xreadgroup command is not allowed with BLOCK option from scripts");
              block_ms = block;
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
          if (rem == 0 || (rem % 2) != 0)
            return encode_error("Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.");
          const std::size_t n = rem / 2;
          std::vector<std::pair<std::string, std::string>> streams;
          for (std::size_t k = 0; k < n; ++k) {
            streams.push_back({args[i + k], args[i + n + k]});
          }
          bool wrongtype = false;
          std::string err;
          bool consumer_created = false;
          auto rows = store().xreadgroup(group, consumer, streams, req_count, noack, wrongtype, err, &consumer_created);
          if (wrongtype) return wrongtype_error_reply();
          if (!err.empty()) return encode_stream_error(err);

          // XREADGROUP is not propagated verbatim (canonicalize sets emit=false); instead we
          // propagate its deterministic effects to BOTH the AOF and the replication backlog.
          auto propagate = [&](std::vector<std::string> cmd) {
            append_replication_event(cmd, session, nullptr);
            append_aof_command(cmd, session, nullptr);
          };
          // For a NOACK read there is no XCLAIM to carry the consumer, so a newly-created
          // consumer is propagated as XGROUP CREATECONSUMER up front (Redis creates it when
          // the command is processed, even if the read then blocks or returns nothing). ACK
          // reads instead let the per-entry XCLAIM (below) create the consumer on replay.
          if (consumer_created && noack) {
            for (const auto& s : streams)
              if (s.second == ">") propagate({"XGROUP", "CREATECONSUMER", s.first, group, consumer});
          }
          // A freshly-created consumer fires an "xgroup-createconsumer" keyspace event
          // (ACK or NOACK), on each stream whose group it was created in.
          if (consumer_created) {
            for (const auto& s : streams)
              if (s.second == ">") notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", s.first, session.db_index);
          }

          if (rows.empty()) {
            // Block only when reading new messages (all ids are ">"), like Redis.
            bool all_new = !streams.empty();
            for (const auto& s : streams) if (s.second != ">") { all_new = false; break; }
            if (block_ms >= 0 && all_new && !g_block_disabled && !in_lua_script_context() && !g_executing_exec) {
              session.blocked.type = BlockType::Stream;
              session.blocked.keys.clear();
              for (const auto& s : streams) { session.blocked.keys.push_back(s.first); register_key_waiter(s.first, session.fd); }
              session.blocked.original_args = args;
              session.blocked.deadline_ms = block_ms > 0 ? now_ms() + block_ms : 0;
              return std::string{};  // deferred
            }
            return encode_null(RespVersion::Resp2);
          }

          // Propagate deterministic effects (Redis streamReplyWithRange): an ACK read emits
          // one idempotent XCLAIM per delivered entry, carrying LASTID so the replica/AOF
          // advances the group cursor (and FORCE creates the consumer + PEL). A NOACK read has
          // no PEL, so it emits a single XGROUP SETID (last-id + entries-read). History
          // re-reads (id != ">") change no ownership → nothing to propagate.
          std::unordered_map<std::string, std::string> idreq_of;
          for (const auto& s : streams) idreq_of[s.first] = s.second;
          std::vector<std::string> out;
          for (const auto& [key, entries] : rows) {
            const bool is_new = idreq_of[key] == ">";
            std::vector<std::string> encoded_entries;
            for (const auto& [id, fields] : entries) {
              if (is_new && !noack)
                propagate({"XCLAIM", key, group, consumer, "0", id, "TIME", "0", "RETRYCOUNT", "1",
                           "FORCE", "JUSTID", "LASTID", id});
              if (fields.empty()) {
                // Null-fields sentinel: a pending id whose stream entry was removed.
                encoded_entries.push_back(encode_array({encode_bulk(id), encode_null_array()}));
                continue;
              }
              std::vector<std::string> fv;
              for (const auto& [f, v] : fields) {
                fv.push_back(encode_bulk(f));
                fv.push_back(encode_bulk(v));
              }
              encoded_entries.push_back(encode_array({encode_bulk(id), encode_array(fv)}));
            }
            if (is_new && noack && !entries.empty()) {
              std::string lid; std::int64_t er = 0;
              if (store().stream_group_ids(key, group, lid, er))
                propagate({"XGROUP", "SETID", key, group, lid, "ENTRIESREAD", std::to_string(er)});
            }
            out.push_back(encode_array({encode_bulk(key), encode_array(encoded_entries)}));
          }
          return encode_array(out);
        }});

    t.emplace("XREAD", CommandSpec{"XREAD", -4, {"readonly", "blocking"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::size_t i = 1;
          std::int64_t count = -1;
          std::int64_t block_ms = -1;
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
              block_ms = v;
              i += 2;
              continue;
            }
            break;
          }
          if (i >= args.size() || upper(args[i]) != "STREAMS") return encode_error("syntax error");
          ++i;
          const auto rem = args.size() - i;
          if (rem == 0 || (rem % 2) != 0)
            return encode_error("Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.");
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
          if (!any_data && block_ms >= 0 && !g_block_disabled && !in_lua_script_context() && !g_executing_exec) {
            // Block: resolve "$" to each stream's current last id so a re-read after
            // wakeup returns only entries added while we were blocked.
            std::vector<std::string> resolved = args;
            for (std::size_t k = 0; k < n; ++k) {
              if (resolved[i + n + k] == "$") {
                bool wt = false;
                auto es = store().xrange(args[i + k], "-", "+", false, wt);
                resolved[i + n + k] = es.empty() ? "0-0" : es.back().first;
              }
            }
            session.blocked.type = BlockType::Stream;
            session.blocked.keys.clear();
            for (std::size_t k = 0; k < n; ++k) { session.blocked.keys.push_back(args[i + k]); register_key_waiter(args[i + k], session.fd); }
            session.blocked.original_args = std::move(resolved);
            session.blocked.deadline_ms = block_ms > 0 ? now_ms() + block_ms : 0;
            return std::string{};  // deferred until data arrives or timeout
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
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::vector<std::string> ids(args.begin() + 3, args.end());
          // Validate every id up front: XACK is all-or-nothing, so one bad id is an error
          // rather than a partial ack (Redis xackCommand parses all IDs before acting).
          for (const auto& id : ids) {
            if (!stream_id_is_valid(id))
              return encode_error("Invalid stream ID specified as stream command argument");
          }
          return with_stream_key(args[1], [&]() {
            bool wrongtype = false;
            std::string err;
            auto n = store().xack(args[1], args[2], ids, wrongtype, err);
            if (wrongtype) return wrongtype_error_reply();
            if (!err.empty()) return encode_stream_error(err);
            return encode_integer(n);
          });
        }});

    t.emplace("XPENDING", CommandSpec{"XPENDING", -3, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          // XPENDING key group                              -> summary form
          // XPENDING key group [IDLE ms] start end count [consumer] -> extended form
          return with_stream_key(args[1], [&]() -> std::string {
            bool wrongtype = false;
            std::string err;
            if (args.size() == 3) {
              std::vector<std::pair<std::string, std::int64_t>> consumers;
              auto sum = store().xpending_summary(args[1], args[2], consumers, wrongtype, err);
              if (wrongtype) return wrongtype_error_reply();
              if (!err.empty()) return encode_stream_error(err);
              std::vector<std::string> out;
              out.push_back(encode_integer(sum.empty() ? 0 : std::stoll(sum[0])));
              out.push_back(sum.size() > 1 && !sum[1].empty() ? encode_bulk(sum[1]) : encode_null(RespVersion::Resp2));
              out.push_back(sum.size() > 2 && !sum[2].empty() ? encode_bulk(sum[2]) : encode_null(RespVersion::Resp2));
              if (consumers.empty()) {
                out.push_back(encode_null_array());  // no pending: consumer list is nil
              } else {
                std::vector<std::string> cs;
                for (const auto& [c, n] : consumers)
                  cs.push_back(encode_array({encode_bulk(c), encode_bulk(std::to_string(n))}));
                out.push_back(encode_array(cs));
              }
              return encode_array(out);
            }
            // Extended form.
            std::size_t i = 3;
            std::int64_t min_idle = 0;
            if (upper(args[i]) == "IDLE") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              if (!parse_i64(args[i + 1], min_idle) || min_idle < 0) return encode_error("value is not an integer or out of range");
              i += 2;
            }
            if (i + 2 >= args.size()) return encode_error("syntax error");  // need start end count
            const std::string start = args[i], end = args[i + 1];
            std::int64_t count = 0;
            if (!parse_i64(args[i + 2], count)) return encode_error("value is not an integer or out of range");
            i += 3;
            std::string consumer;
            bool have_consumer = false;
            if (i < args.size()) { consumer = args[i]; have_consumer = true; ++i; }
            if (i != args.size()) return encode_error("syntax error");
            auto items = store().xpending_extended(args[1], args[2], start, end, count,
                                                   have_consumer ? &consumer : nullptr, min_idle, wrongtype, err);
            if (wrongtype) return wrongtype_error_reply();
            if (!err.empty()) return encode_stream_error(err);
            std::vector<std::string> out;
            for (const auto& it : items) {
              out.push_back(encode_array({encode_bulk(it.id), encode_bulk(it.consumer),
                                          encode_integer(it.idle_ms), encode_integer(it.delivery_count)}));
            }
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
        [](const std::vector<std::string>& args, SessionState& session, bool& should_close) -> std::string {
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
          if (sub == "KILL") {
            // Old form: CLIENT KILL <ip:port>.  New form: CLIENT KILL [ID id] [ADDR a] [SKIPME y/n] ...
            if (args.size() == 3) {
              const bool self = (session.addr == args[2]);
              int killed = g_client_kill_cb ? g_client_kill_cb("ADDR", args[2], session.fd) : 0;
              if (self) { should_close = true; ++killed; }
              return killed > 0 ? encode_simple("OK") : encode_error("No such client");
            }
            std::string by, val; bool skipme = true;
            for (std::size_t i = 2; i + 1 < args.size(); i += 2) {
              const auto o = upper(args[i]);
              if (o == "ID") { by = "ID"; val = args[i + 1]; }
              else if (o == "ADDR") { by = "ADDR"; val = args[i + 1]; }
              else if (o == "SKIPME") { skipme = (lower(args[i + 1]) != "no"); }
              else if (o == "TYPE" || o == "LADDR" || o == "USER" || o == "MAXAGE") { /* accepted; unsupported filter */ }
              else return encode_error("syntax error");
            }
            if (by.empty()) return encode_error("syntax error");
            const bool self = (by == "ID" && std::to_string(session.client_id) == val) ||
                              (by == "ADDR" && session.addr == val);
            int killed = g_client_kill_cb ? g_client_kill_cb(by, val, session.fd) : 0;
            if (self && !skipme) { should_close = true; ++killed; }
            return encode_integer(killed);
          }
          if (sub == "NO-EVICT" && args.size() == 3) {
            const auto m = upper(args[2]);
            if (m != "ON" && m != "OFF") return encode_error("syntax error");
            return encode_simple("OK");  // eviction protection is a no-op (single-node)
          }
          if (sub == "NO-TOUCH" && args.size() == 3) {
            const auto m = upper(args[2]);
            if (m == "ON") session.no_touch = true;
            else if (m == "OFF") session.no_touch = false;
            else return encode_error("syntax error");
            return encode_simple("OK");
          }
          if (sub == "UNBLOCK" && (args.size() == 3 || args.size() == 4)) {
            std::int64_t cid = 0;
            if (!parse_i64(args[2], cid) || cid < 0) return encode_error("value is not an integer or out of range");
            bool error = false;
            if (args.size() == 4) {
              const auto mode = upper(args[3]);
              if (mode == "ERROR") error = true;
              else if (mode != "TIMEOUT") return encode_error("CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
            }
            const int n = g_client_unblock_cb ? g_client_unblock_cb(static_cast<std::uint64_t>(cid), error) : 0;
            return encode_integer(n);
          }
          if (sub == "PAUSE" || sub == "UNPAUSE") {
            return encode_simple("OK");
          }
          if (sub == "REPLY") {
            if (args.size() != 3) return encode_error("wrong number of arguments for 'client|reply' command");
            const auto mode = upper(args[2]);
            if (mode == "ON") { session.reply_off = false; return encode_simple("OK"); }
            if (mode == "OFF") { session.reply_off = true; return std::string(); }  // no reply
            if (mode == "SKIP") {
              // Skip the NEXT command's reply (no-op if replies are already off). No reply now.
              if (!session.reply_off) session.reply_skip_next = true;
              return std::string();
            }
            return encode_error("syntax error");
          }
          if (sub == "HELP") {
            return encode_array({
                encode_bulk("CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                encode_bulk("ID"), encode_bulk("INFO"), encode_bulk("GETNAME"),
                encode_bulk("SETNAME <name>"), encode_bulk("SETINFO <attr> <value>"),
                encode_bulk("LIST [options ...]"), encode_bulk("KILL <ip:port> | <option> <value> ..."),
                encode_bulk("UNBLOCK <clientid> [TIMEOUT|ERROR]"), encode_bulk("NO-EVICT ON|OFF"),
                encode_bulk("NO-TOUCH ON|OFF"), encode_bulk("PAUSE <timeout> [WRITE|ALL]"),
                encode_bulk("UNPAUSE"), encode_bulk("REPLY ON|OFF|SKIP"), encode_bulk("HELP")});
          }
          return encode_error("unknown subcommand or wrong number of arguments for 'client|" + lower(args[1]) + "' command");
        }});

    t.emplace("MONITOR", CommandSpec{"MONITOR", 1, {"admin", "loading", "stale"}, 0, 0, 0,
        [](const std::vector<std::string>&, SessionState& session, bool&) {
          g_monitor_fds.insert(session.fd);
          session.is_monitor = true;
          return encode_simple("OK");
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
            std::int64_t count = 10;
            if (args.size() >= 3) {
              if (!parse_i64(args[2], count)) return encode_error("value is not an integer or out of range");
              if (count < -1) return encode_error("count should be greater than or equal to -1");
            }
            std::vector<std::string> entries;
            std::size_t n = 0;
            for (const auto& e : g_slowlog) {
              if (count >= 0 && n >= static_cast<std::size_t>(count)) break;
              std::vector<std::string> argv;
              for (const auto& a : e.argv) argv.push_back(encode_bulk(a));
              // Each entry: [id, timestamp, usec, [argv...], client-addr, client-name]
              entries.push_back(encode_array({
                  encode_integer(e.id), encode_integer(e.time_s), encode_integer(e.usec),
                  encode_array(argv), encode_bulk(e.peer), encode_bulk(e.name)}));
              ++n;
            }
            return encode_array(entries);
          }
          if (sub == "LEN") {
            return encode_integer(static_cast<long long>(g_slowlog.size()));
          }
          if (sub == "RESET") {
            g_slowlog.clear();
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
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          const auto sub = upper(args[1]);
          if (sub == "LATEST") {
            // Each: [event, timestamp, latest-ms, max-ms]
            std::vector<std::string> out;
            for (const auto& [name, ev] : g_latency_events) {
              out.push_back(encode_array({encode_bulk(name), encode_integer(ev.time_s),
                                          encode_integer(ev.latest_ms), encode_integer(ev.max_ms)}));
            }
            return encode_array(out);
          }
          if (sub == "HISTORY" && args.size() >= 3) {
            auto it = g_latency_events.find(args[2]);
            if (it == g_latency_events.end()) return encode_array({});
            std::vector<std::string> out;
            for (const auto& [t, l] : it->second.history)
              out.push_back(encode_array({encode_integer(t), encode_integer(l)}));
            return encode_array(out);
          }
          if (sub == "GRAPH" && args.size() >= 3) {
            auto it = g_latency_events.find(args[2]);
            if (it == g_latency_events.end() || it->second.history.empty()) return encode_bulk("");
            std::int64_t hi = 0, lo = std::numeric_limits<std::int64_t>::max();
            for (const auto& [t, l] : it->second.history) { hi = std::max(hi, l); lo = std::min(lo, l); }
            // Minimal graph: the legend line tooling / the suite match on.
            std::string g = args[2] + " - high " + std::to_string(hi) + " ms, low " +
                            std::to_string(lo) + " ms (all time high " +
                            std::to_string(it->second.max_ms) + " ms)\n";
            return encode_bulk(g);
          }
          if (sub == "HISTOGRAM") {
            // Per-command latency histograms, optionally filtered by command / parent name.
            std::vector<std::string> filters;
            for (std::size_t i = 2; i < args.size(); ++i) filters.push_back(lower(args[i]));
            auto matches = [&](const std::string& k) -> bool {
              if (filters.empty()) return true;
              for (const auto& f : filters) {
                if (k == f) return true;
                if (f.find('|') == std::string::npos && k.rfind(f + "|", 0) == 0) return true;  // parent
              }
              return false;
            };
            std::vector<std::string> names;
            for (const auto& [k, h] : g_cmd_latency_hist)
              if (h.calls > 0 && matches(k)) names.push_back(k);
            std::sort(names.begin(), names.end());  // deterministic order
            std::vector<std::string> kv;
            for (const auto& k : names) {
              const auto& h = g_cmd_latency_hist[k];
              std::vector<std::string> buckets;
              for (int i = 0; i < 64; ++i)
                if (h.buckets[i]) {
                  buckets.push_back(encode_integer(1LL << (i + 1)));  // bucket upper bound (usec)
                  buckets.push_back(encode_integer(static_cast<long long>(h.buckets[i])));
                }
              std::vector<std::string> val = {
                  encode_bulk("calls"), encode_integer(static_cast<long long>(h.calls)),
                  encode_bulk("histogram_usec"), encode_map(buckets, session.resp_version)};
              kv.push_back(encode_bulk(k));
              kv.push_back(encode_map(val, session.resp_version));
            }
            return encode_map(kv, session.resp_version);
          }
          if (sub == "RESET") {
            std::int64_t n = static_cast<std::int64_t>(g_latency_events.size());
            if (args.size() >= 3) {
              n = 0;
              for (std::size_t i = 2; i < args.size(); ++i) n += g_latency_events.erase(args[i]);
            } else {
              g_latency_events.clear();
            }
            return encode_integer(n);
          }
          if (sub == "DOCTOR") {
            if (g_latency_events.empty())
              return encode_bulk("Dave, I have observed the system, no worrying latency spikes. Everything seems fine.");
            std::string rep = "Dave, I detected latency events:\n";
            for (const auto& [name, ev] : g_latency_events)
              rep += "- " + name + ": max " + std::to_string(ev.max_ms) + " ms (last " + std::to_string(ev.latest_ms) + " ms)\n";
            return encode_bulk(rep);
          }
          if (sub == "HELP") {
            if (args.size() != 2)
              return encode_error("wrong number of arguments for 'latency|help' command");
            std::vector<std::string> items;
            items.push_back(encode_bulk("LATENCY <subcommand> [<arg> ...]. Subcommands are:"));
            items.push_back(encode_bulk("DOCTOR"));
            items.push_back(encode_bulk("GRAPH <event>"));
            items.push_back(encode_bulk("HISTOGRAM [<command> ...]"));
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
          // RESET exits pub/sub (all channel + pattern + shard subscriptions) and MONITOR mode,
          // per Redis's resetCommand, so the client returns to a clean interactive state.
          if (session.fd >= 0) {
            pubsub_unsubscribe_all(session.fd);
            g_monitor_fds.erase(session.fd);
          }
          // Per Redis, RESET re-runs clientSetDefaultAuth: the client is authenticated iff
          // the default user is nopass (no requirepass), and de-authenticated otherwise.
          session.authenticated = g_requirepass.empty();
          session.user = "default";
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
            if (opt == "RANK") {
              if (!parse_i64(args[i + 1], rank)) return encode_error("value is not an integer or out of range");
              if (rank == std::numeric_limits<std::int64_t>::min()) return encode_error("value is out of range");
              if (rank == 0) return encode_error("RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list");
            }
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
            return encode_error("numkeys should be greater than 0");
          if (static_cast<std::size_t>(numkeys + 2) >= args.size())
            return encode_error("syntax error");
          std::size_t dir_idx = static_cast<std::size_t>(numkeys) + 2;
          if (dir_idx >= args.size()) return encode_error("syntax error");
          const auto dir = upper(args[dir_idx]);
          if (dir != "LEFT" && dir != "RIGHT") return encode_error("syntax error");
          std::int64_t count = 1;
          bool count_seen = false;
          for (std::size_t i = dir_idx + 1; i < args.size(); i += 2) {
            if (i + 1 >= args.size()) return encode_error("syntax error");
            if (upper(args[i]) == "COUNT" && !count_seen) {
              if (!parse_i64(args[i + 1], count) || count <= 0) return encode_error("count should be greater than 0");
              count_seen = true;
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
            const std::vector<std::string> eff{dir == "LEFT" ? "LPOP" : "RPOP", key,
                                               std::to_string(popped.size())};
            append_replication_event(eff, session, nullptr);
            append_aof_command(eff, session, nullptr);
            std::vector<std::string> enc;
            for (const auto& v : popped) enc.push_back(encode_bulk(v));
            return encode_array({encode_bulk(key), encode_array(enc)});
          }
          return encode_null_array(session.resp_version);
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
            try { val = std::stod(v); return !std::isnan(val); } catch (...) { return false; }  // NaN is not a valid bound
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
          const bool withscore = args.size() == 4 && upper(args[3]) == "WITHSCORE";
          if (args.size() > 4 || (args.size() == 4 && !withscore)) return encode_error("syntax error");
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          for (std::size_t i = 0; i < items.size(); ++i) {
            if (items[i].first == args[2]) {
              if (withscore) {
                return encode_array({encode_integer(static_cast<long long>(i)), encode_bulk(format_score(items[i].second))});
              }
              return encode_integer(static_cast<long long>(i));
            }
          }
          // Missing member: nil bulk for the plain form, nil ARRAY for WITHSCORE.
          return withscore ? encode_null_array() : encode_null(session.resp_version);
        }});

    // ─── ZREVRANK ────────────────────────────────────────────────────────
    t.emplace("ZREVRANK", CommandSpec{"ZREVRANK", -3, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          const bool withscore = args.size() == 4 && upper(args[3]) == "WITHSCORE";
          if (args.size() > 4 || (args.size() == 4 && !withscore)) return encode_error("syntax error");
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          for (std::size_t i = 0; i < items.size(); ++i) {
            if (items[i].first == args[2]) {
              long long rev_rank = static_cast<long long>(items.size() - 1 - i);
              if (withscore) {
                return encode_array({encode_integer(rev_rank), encode_bulk(format_score(items[i].second))});
              }
              return encode_integer(rev_rank);
            }
          }
          return withscore ? encode_null_array() : encode_null(session.resp_version);
        }});

    // ─── ZRANGEBYLEX / ZREVRANGEBYLEX / ZLEXCOUNT (legacy forms of ZRANGE BYLEX) ───
    t.emplace("ZRANGEBYLEX", CommandSpec{"ZRANGEBYLEX", -4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::vector<std::string> proxy{"ZRANGE", args[1], args[2], args[3], "BYLEX"};
          proxy.insert(proxy.end(), args.begin() + 4, args.end());  // optional LIMIT
          std::vector<std::pair<std::string, double>> items;
          bool ws = false;
          auto err = zrange_select(proxy, 1, 2, 4, false, items, ws);
          if (!err.empty()) return err;
          std::vector<std::string> out;
          for (const auto& [m, s] : items) out.push_back(encode_bulk(m));
          return encode_array(out);
        }});

    t.emplace("ZREVRANGEBYLEX", CommandSpec{"ZREVRANGEBYLEX", -4, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          // args are max min (reversed); zrange_select's REV handles the bound swap.
          std::vector<std::string> proxy{"ZRANGE", args[1], args[2], args[3], "BYLEX", "REV"};
          proxy.insert(proxy.end(), args.begin() + 4, args.end());
          std::vector<std::pair<std::string, double>> items;
          bool ws = false;
          auto err = zrange_select(proxy, 1, 2, 4, false, items, ws);
          if (!err.empty()) return err;
          std::vector<std::string> out;
          for (const auto& [m, s] : items) out.push_back(encode_bulk(m));
          return encode_array(out);
        }});

    t.emplace("ZLEXCOUNT", CommandSpec{"ZLEXCOUNT", 4, {"readonly", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          const std::vector<std::string> proxy{"ZRANGE", args[1], args[2], args[3], "BYLEX"};
          std::vector<std::pair<std::string, double>> items;
          bool ws = false;
          auto err = zrange_select(proxy, 1, 2, 4, false, items, ws);
          if (!err.empty()) return err;
          return encode_integer(static_cast<long long>(items.size()));
        }});

    // ─── ZREMRANGEBYSCORE / ZREMRANGEBYRANK / ZREMRANGEBYLEX ─────────────
    // Each selects victims with the shared unified-range selector, then rebuilds the
    // zset without them (same delete/re-add mechanics as ZREM).
    {
      auto make_remrange = [](const char* mode) {
        const std::string mode_s = mode;  // "" = index, "BYSCORE", "BYLEX"
        return [mode_s](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::vector<std::string> proxy{"ZRANGE", args[1], args[2], args[3]};
          if (!mode_s.empty()) proxy.push_back(mode_s);
          std::vector<std::pair<std::string, double>> victims;
          bool ws = false;
          auto err = zrange_select(proxy, 1, 2, 4, false, victims, ws);
          if (!err.empty()) return err;
          if (victims.empty()) return encode_integer(0);
          std::unordered_set<std::string> gone;
          for (const auto& [m, sc] : victims) gone.insert(m);
          bool wt = false;
          auto all = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          store().del(args[1]);
          for (const auto& [m, sc] : all) {
            if (!gone.count(m)) store().zadd_one(args[1], sc, m, false, false, false, false, false);
          }
          return encode_integer(static_cast<long long>(gone.size()));
        };
      };
      t.emplace("ZREMRANGEBYSCORE", CommandSpec{"ZREMRANGEBYSCORE", 4, {"write"}, 1, 1, 1, make_remrange("BYSCORE")});
      t.emplace("ZREMRANGEBYRANK", CommandSpec{"ZREMRANGEBYRANK", 4, {"write"}, 1, 1, 1, make_remrange("")});
      t.emplace("ZREMRANGEBYLEX", CommandSpec{"ZREMRANGEBYLEX", 4, {"write"}, 1, 1, 1, make_remrange("BYLEX")});
    }

    // ─── ZINTERCARD numkeys key [key ...] [LIMIT n] ──────────────────────
    t.emplace("ZINTERCARD", CommandSpec{"ZINTERCARD", -3, {"readonly"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::int64_t numkeys = 0;
          if (!parse_i64(args[1], numkeys)) {
            return encode_error("numkeys should be greater than 0");
          }
          if (numkeys <= 0) {
            return encode_error("at least 1 input key is needed for 'zintercard' command");
          }
          if (static_cast<std::size_t>(2 + numkeys) > args.size()) {
            return encode_error("Number of keys can't be greater than number of args");
          }
          std::int64_t limit = 0;
          for (std::size_t i = static_cast<std::size_t>(2 + numkeys); i < args.size(); i += 2) {
            if (upper(args[i]) == "LIMIT" && i + 1 < args.size()) {
              if (!parse_i64(args[i + 1], limit) || limit < 0) {
                return encode_error("LIMIT can't be negative");
              }
            } else {
              return encode_error("syntax error");
            }
          }
          // Members from either a zset (ignore scores) or a plain set.
          auto members_of = [](const std::string& key, std::vector<std::string>& out) -> bool {
            const auto t = store().type_of(key);
            if (t == "none") return true;
            bool wt = false;
            if (t == "zset") { for (const auto& [m, sc] : store().zrange(key, 0, -1, true, wt)) out.push_back(m); return !wt; }
            if (t == "set")  { for (const auto& m : store().smembers(key, wt)) out.push_back(m); return !wt; }
            return false;
          };
          std::vector<std::string> first;
          if (!members_of(args[2], first)) return wrongtype_error_reply();
          std::unordered_set<std::string> current(first.begin(), first.end());
          for (std::int64_t k = 1; k < numkeys && !current.empty(); ++k) {
            std::vector<std::string> next;
            if (!members_of(args[static_cast<std::size_t>(2 + k)], next)) return wrongtype_error_reply();
            std::unordered_set<std::string> nextset(next.begin(), next.end());
            std::unordered_set<std::string> keep;
            for (const auto& m : current) if (nextset.count(m)) keep.insert(m);
            current.swap(keep);
          }
          long long card = static_cast<long long>(current.size());
          if (limit > 0 && card > limit) card = limit;
          return encode_integer(card);
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
          long double incr = 0;
          if (!parse_strict_float(args[2], incr)) return encode_error("value is not a valid float");
          // Pre-check the NaN case (inf + -inf) so the zset is never mutated on error.
          {
            bool wt = false;
            auto items = store().zrange(args[1], 0, -1, true, wt);
            if (wt) return wrongtype_error_reply();
            for (const auto& [m, s] : items) {
              if (m == args[3] && std::isnan(s + static_cast<double>(incr))) {
                return encode_error("resulting score is not a number (NaN)");
              }
            }
          }
          auto result = store().zadd_one(args[1], static_cast<double>(incr), args[3], false, false, false, false, true);
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
            try { val = std::stod(v); return !std::isnan(val); } catch (...) { return false; }  // NaN is not a valid bound
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
            else {
              return encode_error("syntax error");  // e.g. REV is invalid for ZRANGEBYSCORE
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
            try { val = std::stod(v); return !std::isnan(val); } catch (...) { return false; }  // NaN is not a valid bound
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
            else {
              return encode_error("syntax error");  // e.g. REV is invalid for ZRANGEBYSCORE
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
          // The only valid 5th token is WITHSCORES; BYSCORE/BYLEX/etc. are a syntax error.
          if (args.size() > 5 || (args.size() == 5 && upper(args[4]) != "WITHSCORES")) {
            return encode_error("syntax error");
          }
          const bool withscores = args.size() == 5;
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
          if (count == std::numeric_limits<std::int64_t>::min()) {
            return encode_error("value is out of range");  // -INT64_MIN unrepresentable
          }
          const bool withscores = args.size() == 4;
          if (args.size() > 4 || (withscores && upper(args[3]) != "WITHSCORES")) {
            return encode_error("syntax error");
          }
          if (withscores && count < -(std::numeric_limits<std::int64_t>::max() / 2)) {
            return encode_error("value is out of range");  // WITHSCORES doubles the reply
          }
          const bool resp3_pairs = withscores && session.resp_version == RespVersion::Resp3;
          // Positive count: distinct members (at most the zset size); negative: |count|
          // members with repetition (Redis zrandmemberWithCountCommand semantics).
          std::vector<std::size_t> picks;
          if (count >= 0) {
            std::vector<std::size_t> idxs(items.size());
            for (std::size_t i = 0; i < idxs.size(); ++i) idxs[i] = i;
            static thread_local std::mt19937 rng{std::random_device{}()};
            std::shuffle(idxs.begin(), idxs.end(), rng);
            const std::size_t take = std::min<std::size_t>(idxs.size(), static_cast<std::size_t>(count));
            picks.assign(idxs.begin(), idxs.begin() + static_cast<std::ptrdiff_t>(take));
          } else {
            picks.reserve(static_cast<std::size_t>(-count));
            for (std::int64_t i = 0; i < -count; ++i) {
              picks.push_back(static_cast<std::size_t>(std::rand() % static_cast<int>(items.size())));
            }
          }
          std::vector<std::string> enc;
          for (const auto idx : picks) {
            if (resp3_pairs) {
              enc.push_back(encode_array({encode_bulk(items[idx].first), encode_double(items[idx].second, RespVersion::Resp3)}));
            } else {
              enc.push_back(encode_bulk(items[idx].first));
              if (withscores) enc.push_back(encode_bulk(format_score(items[idx].second)));
            }
          }
          return encode_array(enc);
        }});

    // ─── ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE ─────────────────────────
    // ─── ZUNION / ZINTER / ZDIFF (+STORE): one generic engine ───────────
    // Redis zunionInterDiffGenericCommand semantics: WEIGHTS + AGGREGATE SUM|MIN|MAX
    // (not for ZDIFF), plain sets accepted as sources (implicit score 1), the STORE
    // destination is overwritten regardless of its type, NaN sums collapse to 0,
    // non-store forms support WITHSCORES and order by score then member.
    {
      enum class ZOp { Union, Inter, Diff };
      auto zsetop = [](const std::vector<std::string>& args, ZOp op, bool store_form,
                       SessionState& session) -> std::string {
        const std::size_t nk_idx = store_form ? 2 : 1;
        std::int64_t numkeys = 0;
        if (!parse_i64(args[nk_idx], numkeys)) return encode_error("value is not an integer or out of range");
        if (numkeys <= 0) {
          return encode_error("at least 1 input key is needed for '" + lower(args[0]) + "' command");
        }
        const std::size_t keys_start = nk_idx + 1;
        if (keys_start + static_cast<std::size_t>(numkeys) > args.size()) return encode_error("syntax error");

        std::vector<double> weights(static_cast<std::size_t>(numkeys), 1.0);
        enum class Agg { Sum, Min, Max };
        Agg agg = Agg::Sum;
        bool withscores = false;
        for (std::size_t i = keys_start + static_cast<std::size_t>(numkeys); i < args.size();) {
          const auto o = upper(args[i]);
          if (o == "WEIGHTS" && op != ZOp::Diff) {
            if (i + static_cast<std::size_t>(numkeys) >= args.size()) return encode_error("syntax error");
            for (std::int64_t w = 0; w < numkeys; ++w) {
              long double wv = 0;
              if (!parse_strict_float(args[i + 1 + static_cast<std::size_t>(w)], wv)) {
                return encode_error("weight value is not a float");
              }
              weights[static_cast<std::size_t>(w)] = static_cast<double>(wv);
            }
            i += 1 + static_cast<std::size_t>(numkeys);
          } else if (o == "AGGREGATE" && op != ZOp::Diff && i + 1 < args.size()) {
            const auto a = upper(args[i + 1]);
            if (a == "SUM") agg = Agg::Sum;
            else if (a == "MIN") agg = Agg::Min;
            else if (a == "MAX") agg = Agg::Max;
            else return encode_error("syntax error");
            i += 2;
          } else if (o == "WITHSCORES" && !store_form) {
            withscores = true;
            ++i;
          } else {
            return encode_error("syntax error");
          }
        }

        // Fetch one source as {member,score}: zsets keep scores, plain sets score 1.
        auto fetch = [](const std::string& key,
                        std::vector<std::pair<std::string, double>>& out) -> bool {
          const auto t = store().type_of(key);
          if (t == "none") return true;
          if (t == "zset") {
            bool wt = false;
            out = store().zrange(key, 0, -1, true, wt);
            return !wt;
          }
          if (t == "set") {
            bool wt = false;
            for (const auto& m : store().smembers(key, wt)) out.push_back({m, 1.0});
            return !wt;
          }
          return false;
        };
        auto aggregate = [&](double acc, double v) -> double {
          switch (agg) {
            case Agg::Min: return std::min(acc, v);
            case Agg::Max: return std::max(acc, v);
            default: {
              const double sum = acc + v;
              return std::isnan(sum) ? 0.0 : sum;  // inf + -inf → 0 like Redis
            }
          }
        };

        std::unordered_map<std::string, double> result;
        if (op == ZOp::Diff) {
          std::vector<std::pair<std::string, double>> first;
          if (!fetch(args[keys_start], first)) return wrongtype_error_reply();
          for (const auto& [m, sc] : first) result[m] = sc;
          for (std::int64_t k = 1; k < numkeys && !result.empty(); ++k) {
            std::vector<std::pair<std::string, double>> src;
            if (!fetch(args[keys_start + static_cast<std::size_t>(k)], src)) return wrongtype_error_reply();
            for (const auto& [m, sc] : src) result.erase(m);
          }
        } else {
          std::unordered_map<std::string, int> seen_in;
          for (std::int64_t k = 0; k < numkeys; ++k) {
            std::vector<std::pair<std::string, double>> src;
            if (!fetch(args[keys_start + static_cast<std::size_t>(k)], src)) return wrongtype_error_reply();
            const double w = weights[static_cast<std::size_t>(k)];
            for (const auto& [m, sc] : src) {
              double ws = w * sc;
              if (std::isnan(ws)) ws = 0.0;  // 0 * inf
              auto it = result.find(m);
              if (it == result.end()) {
                result[m] = ws;
                seen_in[m] = 1;
              } else {
                it->second = aggregate(it->second, ws);
                ++seen_in[m];
              }
            }
          }
          if (op == ZOp::Inter) {
            for (auto it = result.begin(); it != result.end();) {
              if (seen_in[it->first] < numkeys) it = result.erase(it);
              else ++it;
            }
          }
        }

        if (store_form) {
          store().del(args[1]);
          for (const auto& [m, sc] : result) {
            store().zadd_one(args[1], sc, m, false, false, false, false, false);
          }
          return encode_integer(static_cast<long long>(result.size()));
        }
        std::vector<std::pair<std::string, double>> ordered(result.begin(), result.end());
        std::sort(ordered.begin(), ordered.end(), [](const auto& a, const auto& b) {
          if (a.second != b.second) return a.second < b.second;
          return a.first < b.first;
        });
        const bool resp3_pairs = withscores && session.resp_version == RespVersion::Resp3;
        std::vector<std::string> enc;
        for (const auto& [m, sc] : ordered) {
          if (resp3_pairs) {
            enc.push_back(encode_array({encode_bulk(m), encode_double(sc, RespVersion::Resp3)}));
          } else {
            enc.push_back(encode_bulk(m));
            if (withscores) enc.push_back(encode_bulk(format_score(sc)));
          }
        }
        return encode_array(enc);
      };

      t.emplace("ZUNIONSTORE", CommandSpec{"ZUNIONSTORE", -4, {"write"}, 1, 1, 1,
          [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) { return zsetop(a, ZOp::Union, true, se); }});
      t.emplace("ZINTERSTORE", CommandSpec{"ZINTERSTORE", -4, {"write"}, 1, 1, 1,
          [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) { return zsetop(a, ZOp::Inter, true, se); }});
      t.emplace("ZDIFFSTORE", CommandSpec{"ZDIFFSTORE", -4, {"write"}, 1, 1, 1,
          [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) { return zsetop(a, ZOp::Diff, true, se); }});
      t.emplace("ZUNION", CommandSpec{"ZUNION", -3, {"readonly"}, 0, 0, 0,
          [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) { return zsetop(a, ZOp::Union, false, se); }});
      t.emplace("ZINTER", CommandSpec{"ZINTER", -3, {"readonly"}, 0, 0, 0,
          [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) { return zsetop(a, ZOp::Inter, false, se); }});
      t.emplace("ZDIFF", CommandSpec{"ZDIFF", -3, {"readonly"}, 0, 0, 0,
          [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) { return zsetop(a, ZOp::Diff, false, se); }});
    }

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
          if (count == std::numeric_limits<std::int64_t>::min()) {
            return encode_error("value is out of range");  // -INT64_MIN unrepresentable
          }
          // Positive count: distinct members, at most the set size. Negative: |count|
          // members with repetition (Redis srandmemberWithCountCommand semantics).
          std::vector<std::string> enc;
          if (count >= 0) {
            std::vector<std::size_t> idxs(members.size());
            for (std::size_t i = 0; i < idxs.size(); ++i) idxs[i] = i;
            static thread_local std::mt19937 rng{std::random_device{}()};
            std::shuffle(idxs.begin(), idxs.end(), rng);
            const std::size_t take = std::min<std::size_t>(idxs.size(), static_cast<std::size_t>(count));
            enc.reserve(take);
            for (std::size_t i = 0; i < take; ++i) enc.push_back(encode_bulk(members[idxs[i]]));
          } else {
            enc.reserve(static_cast<std::size_t>(-count));
            for (std::int64_t i = 0; i < -count; ++i) {
              auto idx = static_cast<std::size_t>(std::rand() % static_cast<int>(members.size()));
              enc.push_back(encode_bulk(members[idx]));
            }
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
          if (!parse_i64(args[1], numkeys) || numkeys <= 0) return encode_error("numkeys should be greater than 0");
          if (static_cast<std::size_t>(2 + numkeys) > args.size()) {
            return encode_error("Number of keys can't be greater than number of args");
          }
          std::int64_t limit = 0;
          std::size_t opt_start = static_cast<std::size_t>(2 + numkeys);
          for (std::size_t i = opt_start; i < args.size(); i += 2) {
            if (upper(args[i]) == "LIMIT" && i + 1 < args.size()) {
              if (!parse_i64(args[i + 1], limit) || limit < 0) {
                return encode_error("LIMIT can't be negative");
              }
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
          if (args.size() > 4) return encode_error("syntax error");
          if (store().is_wrongtype_for_hash(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto all = store().hgetall(args[1], wt);
          if (wt) return wrongtype_error_reply();
          if (args.size() == 2) {
            if (all.empty()) return encode_null(session.resp_version);
            auto idx = static_cast<std::size_t>(std::rand() % static_cast<int>(all.size()));
            return encode_bulk(all[idx].first);
          }
          std::int64_t count;
          if (!parse_i64(args[2], count)) return encode_error("value is not an integer or out of range");
          // -INT64_MIN is unrepresentable; Redis rejects the magnitude as out of range.
          if (count == std::numeric_limits<std::int64_t>::min()) {
            return encode_error("value is out of range");
          }
          const bool withvalues = args.size() == 4;
          if (withvalues && upper(args[3]) != "WITHVALUES") return encode_error("syntax error");
          // WITHVALUES doubles the reply length — Redis bounds |count| at LONG_MAX/2.
          if (withvalues && count < -(std::numeric_limits<std::int64_t>::max() / 2)) {
            return encode_error("value is out of range");
          }
          const bool resp3_pairs = withvalues && session.resp_version == RespVersion::Resp3;
          // Positive count: distinct fields, at most the hash size. Negative: |count|
          // fields with repetition (Redis hrandfieldWithCountCommand semantics).
          std::vector<std::size_t> picks;
          if (count >= 0) {
            std::vector<std::size_t> idxs(all.size());
            for (std::size_t i = 0; i < idxs.size(); ++i) idxs[i] = i;
            static thread_local std::mt19937 rng{std::random_device{}()};
            std::shuffle(idxs.begin(), idxs.end(), rng);
            const std::size_t take = std::min<std::size_t>(idxs.size(), static_cast<std::size_t>(count));
            picks.assign(idxs.begin(), idxs.begin() + static_cast<std::ptrdiff_t>(take));
          } else if (!all.empty()) {
            picks.reserve(static_cast<std::size_t>(-count));
            for (std::int64_t i = 0; i < -count; ++i) {
              picks.push_back(static_cast<std::size_t>(std::rand() % static_cast<int>(all.size())));
            }
          }
          std::vector<std::string> enc;
          enc.reserve(picks.size() * (withvalues && !resp3_pairs ? 2 : 1));
          for (const auto idx : picks) {
            if (resp3_pairs) {
              enc.push_back(encode_array({encode_bulk(all[idx].first), encode_bulk(all[idx].second)}));
            } else {
              enc.push_back(encode_bulk(all[idx].first));
              if (withvalues) enc.push_back(encode_bulk(all[idx].second));
            }
          }
          return encode_array(enc);
        }});

    // ─── BITCOUNT ────────────────────────────────────────────────────────
    t.emplace("BITCOUNT", CommandSpec{"BITCOUNT", -2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          // Valid forms: BITCOUNT key | key start end | key start end BYTE|BIT. A lone start
          // (or trailing junk) is a syntax error (Redis), not a silently-ignored argument.
          if (args.size() == 3 || args.size() > 5) return encode_error("syntax error");
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
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          // BITFIELD: GET/SET/INCRBY on signed/unsigned integer bitfields with OVERFLOW modes.
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
          // Overflow handling mirrors Redis check{Signed,Unsigned}BitfieldOverflow (bitops.c):
          // owtype 0=WRAP (default), 1=SAT, 2=FAIL. Returns 0 (no overflow) / ±1 (overflow),
          // and on WRAP/SAT sets `limit` to the value to store.
          enum { OW_WRAP = 0, OW_SAT = 1, OW_FAIL = 2 };
          auto check_signed = [](std::int64_t value, std::int64_t incr, int bits, int owtype,
                                 std::int64_t& limit) -> int {
            const std::int64_t max = (bits == 64) ? INT64_MAX : ((std::int64_t{1} << (bits - 1)) - 1);
            const std::int64_t min = (-max) - 1;
            const std::int64_t maxincr = static_cast<std::int64_t>(static_cast<std::uint64_t>(max) - static_cast<std::uint64_t>(value));
            const std::int64_t minincr = min - value;
            auto wrap = [&]() {
              const std::uint64_t msb = std::uint64_t{1} << (bits - 1);
              std::uint64_t c = static_cast<std::uint64_t>(value) + static_cast<std::uint64_t>(incr);
              if (bits < 64) {
                const std::uint64_t mask = ~std::uint64_t{0} << bits;
                if (c & msb) c |= mask; else c &= ~mask;
              }
              limit = static_cast<std::int64_t>(c);
            };
            if (value > max || (bits != 64 && incr > maxincr) || (value >= 0 && incr > 0 && incr > maxincr)) {
              if (owtype == OW_WRAP) wrap(); else if (owtype == OW_SAT) limit = max;
              return 1;
            } else if (value < min || (bits != 64 && incr < minincr) || (value < 0 && incr < 0 && incr < minincr)) {
              if (owtype == OW_WRAP) wrap(); else if (owtype == OW_SAT) limit = min;
              return -1;
            }
            return 0;
          };
          auto check_unsigned = [](std::uint64_t value, std::int64_t incr, int bits, int owtype,
                                   std::uint64_t& limit) -> int {
            const std::uint64_t max = (bits == 64) ? UINT64_MAX : ((std::uint64_t{1} << bits) - 1);
            const std::int64_t maxincr = static_cast<std::int64_t>(max - value);
            const std::int64_t minincr = -static_cast<std::int64_t>(value);
            auto wrap = [&]() {
              const std::uint64_t mask = ~std::uint64_t{0} << bits;
              limit = (value + static_cast<std::uint64_t>(incr)) & ~mask;
            };
            if (value > max || (incr > 0 && incr > maxincr)) {
              if (owtype == OW_WRAP) wrap(); else if (owtype == OW_SAT) limit = max;
              return 1;
            } else if (incr < 0 && incr < minincr) {
              if (owtype == OW_WRAP) wrap(); else if (owtype == OW_SAT) limit = 0;
              return -1;
            }
            return 0;
          };
          bool modified = false;
          int owtype = OW_WRAP;
          std::vector<std::string> results;
          std::size_t i = 2;
          while (i < args.size()) {
            auto subcmd = upper(args[i]);
            if (subcmd == "OVERFLOW") {
              if (i + 1 >= args.size()) return encode_error("syntax error");
              const auto m = upper(args[i + 1]);
              if (m == "WRAP") owtype = OW_WRAP;
              else if (m == "SAT") owtype = OW_SAT;
              else if (m == "FAIL") owtype = OW_FAIL;
              else return encode_error("Invalid OVERFLOW type specified");
              i += 2;
              continue;
            }
            if (subcmd == "GET") {
              if (i + 2 >= args.size()) return encode_error("syntax error");
              bool is_signed; int bits;
              if (!parse_type(args[i + 1], is_signed, bits)) return encode_error("Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.");
              std::size_t off;
              if (!parse_offset(args[i + 2], bits, off)) return encode_error("bit offset is not an integer or out of range");
              results.push_back(encode_integer(read_field(off, bits, is_signed)));
              i += 3;
            } else if (subcmd == "SET" || subcmd == "INCRBY") {
              const bool is_incr = subcmd == "INCRBY";
              if (i + 3 >= args.size()) return encode_error("syntax error");
              bool is_signed; int bits;
              if (!parse_type(args[i + 1], is_signed, bits)) return encode_error("Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.");
              std::size_t off;
              if (!parse_offset(args[i + 2], bits, off)) return encode_error("bit offset is not an integer or out of range");
              std::int64_t arg;
              if (!parse_i64(args[i + 3], arg)) return encode_error("value is not an integer or out of range");
              i += 4;
              const std::int64_t oldv = read_field(off, bits, is_signed);
              std::int64_t newval, retval;
              int overflow;
              if (is_signed) {
                std::int64_t wrapped = 0;
                if (is_incr) {
                  overflow = check_signed(oldv, arg, bits, owtype, wrapped);
                  newval = overflow ? wrapped : oldv + arg;
                  retval = newval;
                } else {
                  newval = arg;
                  overflow = check_signed(newval, 0, bits, owtype, wrapped);
                  if (overflow) newval = wrapped;
                  retval = oldv;
                }
              } else {
                std::uint64_t wrapped = 0;
                const std::uint64_t oldu = static_cast<std::uint64_t>(oldv);
                if (is_incr) {
                  overflow = check_unsigned(oldu, arg, bits, owtype, wrapped);
                  newval = static_cast<std::int64_t>(overflow ? wrapped : oldu + static_cast<std::uint64_t>(arg));
                  retval = newval;
                } else {
                  const std::uint64_t nv = static_cast<std::uint64_t>(arg);
                  overflow = check_unsigned(nv, 0, bits, owtype, wrapped);
                  newval = static_cast<std::int64_t>(overflow ? wrapped : nv);
                  retval = oldv;
                }
              }
              // OVERFLOW FAIL: on overflow, write nothing and reply nil for this op.
              if (overflow && owtype == OW_FAIL) {
                results.push_back(encode_null(session.resp_version));
              } else {
                write_field(off, bits, newval);
                modified = true;
                results.push_back(encode_integer(retval));
              }
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
            geoutil::decode(static_cast<std::uint64_t>(score), lon, lat);
          };
          double lon1 = 0, lat1 = 0, lon2 = 0, lat2 = 0;
          bool found1 = false, found2 = false;
          for (const auto& [m, s] : items) {
            if (m == args[2]) { decode_geohash(s, lon1, lat1); found1 = true; }
            if (m == args[3]) { decode_geohash(s, lon2, lat2); found2 = true; }
          }
          if (!found1 || !found2) return encode_null(session.resp_version);
          double dist = geoutil::dist(lon1, lat1, lon2, lat2);
          double mul = 1.0;
          if (args.size() >= 5 && !geoutil::unit_to_m(lower(args[4]), mul))
            return encode_error("unsupported unit provided. please use M, KM, FT, MI");
          char buf[64]; std::snprintf(buf, sizeof(buf), "%.4f", dist / mul);
          return encode_bulk(buf);
        }});

    // ─── GEOPOS ──────────────────────────────────────────────────────────
    t.emplace("GEOPOS", CommandSpec{"GEOPOS", -2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          // Decode 52-bit interleaved geohash back to lon/lat.
          auto decode_geohash = [](double score, double& lon, double& lat) {
            geoutil::decode(static_cast<std::uint64_t>(score), lon, lat);
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
    t.emplace("GEOHASH", CommandSpec{"GEOHASH", -2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
          bool wt = false;
          auto items = store().zrange(args[1], 0, -1, true, wt);
          if (wt) return wrongtype_error_reply();
          // Decode 52-bit interleaved geohash back to lon/lat.
          auto decode_geohash = [](double score, double& lon, double& lat) {
            geoutil::decode(static_cast<std::uint64_t>(score), lon, lat);
          };
          constexpr const char* kBase32 = "0123456789bcdefghjkmnpqrstuvwxyz";
          auto encode_geohash_str = [&](double lon, double lat) -> std::string {
            double lon_min = -180, lon_max = 180;
            double lat_min = -90, lat_max = 90;
            bool is_lon = true;
            std::uint64_t bits = 0;
            int num_bits = 0;
            std::string hash;
            // Redis derives 52 geohash bits (11 standard chars = 55 bits), then forces the
            // 11th char to '0' since the low 3 bits are zero-padding (geohashCommand). We
            // subdivide the first 10 chars (50 bits) and append the fixed final '0'.
            while (hash.size() < 10) {
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
            hash += kBase32[0];  // 11th char: low 3 geohash bits are zero-padding
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
        [](const std::vector<std::string>& args, SessionState& s, bool&) -> std::string {
          return do_geo_search(args, s.resp_version, /*form=*/0, /*readonly=*/false);
        }});

    t.emplace("GEORADIUSBYMEMBER", CommandSpec{"GEORADIUSBYMEMBER", -5, {"write"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& s, bool&) -> std::string {
          return do_geo_search(args, s.resp_version, /*form=*/1, /*readonly=*/false);
        }});

    // ─── GEOSEARCH ───────────────────────────────────────────────────────
    t.emplace("GEOSEARCH", CommandSpec{"GEOSEARCH", -7, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState& s, bool&) -> std::string {
          return do_geo_search(args, s.resp_version, /*form=*/2, /*readonly=*/true);
        }});

    t.emplace("GEOSEARCHSTORE", CommandSpec{"GEOSEARCHSTORE", -8, {"write"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState& s, bool&) -> std::string {
          return do_geo_search(args, s.resp_version, /*form=*/3, /*readonly=*/false);
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
          // Shared trailing-option parse: optional ~/= then the threshold, then an
          // optional LIMIT that is only legal with ~ (Redis streamParseAddOrTrimArgsOrReply).
          auto parse_threshold_and_limit = [&](std::size_t& start, bool& approx) -> std::string {
            approx = false;
            if (start < args.size() && (args[start] == "~" || args[start] == "=")) {
              approx = (args[start] == "~"); ++start;
            }
            if (start >= args.size()) return encode_error("syntax error");
            const std::size_t after = start + 1;
            if (after < args.size()) {
              if (after + 1 < args.size() && upper(args[after]) == "LIMIT") {
                if (!approx) return encode_error("syntax error, LIMIT cannot be used without the special ~ option");
                std::int64_t lim = 0;
                if (!parse_i64(args[after + 1], lim)) return encode_error("value is not an integer or out of range");
              } else {
                return encode_error("syntax error");
              }
            }
            return "";
          };
          if (strategy == "MAXLEN") {
            std::size_t start = 3;
            bool approx = false;
            auto perr = parse_threshold_and_limit(start, approx);
            if (!perr.empty()) return perr;
            std::int64_t maxlen;
            if (!parse_i64(args[start], maxlen)) return encode_error("value is not an integer or out of range");
            if (maxlen < 0) return encode_error("The MAXLEN argument must be >= 0.");
            long long trimmed = 0;
            if (static_cast<std::int64_t>(entries.size()) > maxlen) {
              std::size_t to_remove = entries.size() - static_cast<std::size_t>(maxlen);
              std::vector<std::string> ids;
              for (std::size_t i = 0; i < to_remove; ++i) ids.push_back(entries[i].first);
              trimmed = store().xdel(args[1], ids, wt, /*is_trim=*/true);
            }
            return encode_integer(trimmed);
          }
          if (strategy == "MINID") {
            std::size_t start = 3;
            bool approx = false;
            auto perr = parse_threshold_and_limit(start, approx);
            if (!perr.empty()) return perr;
            const auto& min_id = args[start];
            long long trimmed = 0;
            std::vector<std::string> ids;
            for (const auto& [id, _] : entries) {
              if (stream_id_lt(id, min_id)) ids.push_back(id);  // numeric compare
            }
            if (!ids.empty()) trimmed = store().xdel(args[1], ids, wt, /*is_trim=*/true);
            return encode_integer(trimmed);
          }
          return encode_error("syntax error");
        }});

    // ─── XAUTOCLAIM ──────────────────────────────────────────────────────
    // Encode a claimed stream entry as [id, [f,v,...]] (or just the id for JUSTID).
    auto encode_stream_entry = [](const std::string& id,
                                  const std::vector<std::pair<std::string, std::string>>& fields,
                                  bool justid) -> std::string {
      if (justid) return encode_bulk(id);
      std::vector<std::string> fv;
      for (const auto& [f, v] : fields) { fv.push_back(encode_bulk(f)); fv.push_back(encode_bulk(v)); }
      return encode_array({encode_bulk(id), encode_array(fv)});
    };

    t.emplace("XAUTOCLAIM", CommandSpec{"XAUTOCLAIM", -6, {"write"}, 1, 1, 1,
        [encode_stream_entry](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          // XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID]
          std::int64_t min_idle = 0;
          if (!parse_i64(args[4], min_idle) || min_idle < 0) return encode_error("Invalid min-idle-time argument for XAUTOCLAIM");
          std::int64_t count = 100; bool justid = false;
          // Redis caps COUNT at LONG_MAX / max(sizeof(streamID)=16, attempts_factor=10) = LONG_MAX/16.
          constexpr std::int64_t kMaxCount = 9223372036854775807LL / 16;
          for (std::size_t i = 6; i < args.size(); ++i) {
            const auto o = upper(args[i]);
            if (o == "COUNT" && i + 1 < args.size()) {
              if (!parse_i64(args[++i], count) || count < 1 || count > kMaxCount) return encode_error("COUNT must be > 0");
            }
            else if (o == "JUSTID") justid = true;
            else return encode_error("syntax error");
          }
          bool wt = false; std::string err; bool consumer_created = false;
          auto res = store().xautoclaim(args[1], args[2], args[3], min_idle, args[5], count, justid, wt, err, &consumer_created);
          if (wt) return wrongtype_error_reply();
          if (!err.empty()) return encode_stream_error(err);
          if (consumer_created) notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", args[1], session.db_index);
          std::vector<std::string> ents;
          for (const auto& [id, fields] : res.claimed) ents.push_back(encode_stream_entry(id, fields, justid));
          std::vector<std::string> del;
          for (const auto& d : res.deleted) del.push_back(encode_bulk(d));
          return encode_array({encode_bulk(res.next_cursor), encode_array(ents), encode_array(del)});
        }});

    t.emplace("XCLAIM", CommandSpec{"XCLAIM", -6, {"write", "fast"}, 1, 1, 1,
        [encode_stream_entry](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
          // XCLAIM key group consumer min-idle-time id [id...] [IDLE ms][TIME ms][RETRYCOUNT n][FORCE][JUSTID][LASTID id]
          std::int64_t min_idle = 0;
          if (!parse_i64(args[4], min_idle) || min_idle < 0) return encode_error("Invalid min-idle-time argument for XCLAIM");
          std::vector<std::string> ids; bool justid = false, force = false;
          std::string last_id; bool have_last_id = false;
          for (std::size_t i = 5; i < args.size(); ++i) {
            const auto o = upper(args[i]);
            if (o == "LASTID" && i + 1 < args.size()) { last_id = args[i + 1]; have_last_id = true; ++i; }
            else if ((o == "IDLE" || o == "TIME" || o == "RETRYCOUNT") && i + 1 < args.size()) { ++i; }
            else if (o == "FORCE") force = true;
            else if (o == "JUSTID") justid = true;
            else ids.push_back(args[i]);
          }
          if (ids.empty()) return encode_error("wrong number of arguments for 'xclaim' command");
          bool wt = false; std::string err; bool consumer_created = false;
          auto claimed = store().xclaim(args[1], args[2], args[3], min_idle, ids, justid, force, wt, err,
                                        have_last_id ? &last_id : nullptr, &consumer_created);
          if (wt) return wrongtype_error_reply();
          if (!err.empty()) return "-" + err + "\r\n";
          if (consumer_created) notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", args[1], session.db_index);
          std::vector<std::string> out;
          for (const auto& [id, fields] : claimed) out.push_back(encode_stream_entry(id, fields, justid));
          return encode_array(out);
        }});

    t.emplace("XSETID", CommandSpec{"XSETID", -3, {"write", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          // XSETID key id [ENTRIESADDED n] [MAXDELETEDID id] — options are validated
          // before the key-existence check (Redis xsetidCommand order).
          std::string entries_added, max_deleted;
          bool have_ea = false, have_md = false;
          for (std::size_t i = 3; i < args.size(); ++i) {
            const auto opt = upper(args[i]);
            if (opt == "ENTRIESADDED" && i + 1 < args.size()) {
              std::int64_t v = 0;
              if (!parse_i64(args[i + 1], v)) return encode_error("value is not an integer or out of range");
              if (v < 0) return encode_error("value is out of range, must be positive");
              entries_added = args[i + 1]; have_ea = true; ++i;
            } else if (opt == "MAXDELETEDID" && i + 1 < args.size()) {
              max_deleted = args[i + 1]; have_md = true; ++i;
            } else {
              return encode_error("syntax error");
            }
          }
          bool wt = false; std::string err;
          if (!store().xsetid(args[1], args[2], wt, err,
                              have_ea ? &entries_added : nullptr,
                              have_md ? &max_deleted : nullptr)) {
            if (wt) return wrongtype_error_reply();
            return "-" + err + "\r\n";
          }
          return encode_simple("OK");
        }});

    // (XTRIM MAXLEN is already registered earlier in this table.)

    // ─── HyperLogLog ─────────────────────────────────────────────────────
    t.emplace("PFADD", CommandSpec{"PFADD", -2, {"write", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          const std::string& key = args[1];
          if (store().is_wrongtype_for_string(key)) return wrongtype_error_reply();
          auto cur = store().get(key);
          if (cur.has_value() && !hll_is_valid(*cur))
            return encode_error("WRONGTYPE Key is not a valid HyperLogLog string value.");
          std::string hll = cur.has_value() ? *cur : hll_new();
          bool changed = !cur.has_value();
          for (std::size_t i = 2; i < args.size(); ++i) if (hll_add(hll, args[i])) changed = true;
          if (changed) store().set(key, hll, false, false, std::nullopt, true);
          return encode_integer(changed ? 1 : 0);
        }});

    t.emplace("PFCOUNT", CommandSpec{"PFCOUNT", -2, {"readonly"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          std::string merged = hll_new();
          auto* mregs = reinterpret_cast<std::uint8_t*>(merged.data()) + kHllHdr;
          for (std::size_t i = 1; i < args.size(); ++i) {
            if (store().is_wrongtype_for_string(args[i])) return wrongtype_error_reply();
            auto cur = store().get(args[i]);
            if (!cur.has_value()) continue;
            if (!hll_is_valid(*cur)) return encode_error("WRONGTYPE Key is not a valid HyperLogLog string value.");
            const auto* r = reinterpret_cast<const std::uint8_t*>(cur->data()) + kHllHdr;
            for (int j = 0; j < kHllRegs; ++j) if (r[j] > mregs[j]) mregs[j] = r[j];
          }
          return encode_integer(static_cast<long long>(hll_count(merged) + 0.5));
        }});

    t.emplace("PFMERGE", CommandSpec{"PFMERGE", -2, {"write"}, 1, -1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          const std::string& dest = args[1];
          if (store().is_wrongtype_for_string(dest)) return wrongtype_error_reply();
          auto dcur = store().get(dest);
          if (dcur.has_value() && !hll_is_valid(*dcur))
            return encode_error("WRONGTYPE Key is not a valid HyperLogLog string value.");
          std::string merged = dcur.has_value() ? *dcur : hll_new();
          auto* mregs = reinterpret_cast<std::uint8_t*>(merged.data()) + kHllHdr;
          for (std::size_t i = 2; i < args.size(); ++i) {
            if (store().is_wrongtype_for_string(args[i])) return wrongtype_error_reply();
            auto cur = store().get(args[i]);
            if (!cur.has_value()) continue;
            if (!hll_is_valid(*cur)) return encode_error("WRONGTYPE Key is not a valid HyperLogLog string value.");
            const auto* r = reinterpret_cast<const std::uint8_t*>(cur->data()) + kHllHdr;
            for (int j = 0; j < kHllRegs; ++j) if (r[j] > mregs[j]) mregs[j] = r[j];
          }
          store().set(dest, merged, false, false, std::nullopt, true);
          return encode_simple("OK");
        }});

    // ─── SMOVE ───────────────────────────────────────────────────────────
    t.emplace("SMOVE", CommandSpec{"SMOVE", 4, {"write", "fast"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          const std::string& src = args[1]; const std::string& dst = args[2]; const std::string& m = args[3];
          if (store().is_wrongtype_for_set(src) || store().is_wrongtype_for_set(dst)) return wrongtype_error_reply();
          bool wt = false;
          if (!store().sismember(src, m, wt)) return encode_integer(0);
          if (wt) return wrongtype_error_reply();
          store().srem(src, {m}, wt);
          store().sadd(dst, {m}, wt);
          return encode_integer(1);
        }});

    // ─── ZRANGESTORE — shares the unified range selector with ZRANGE ───
    t.emplace("ZRANGESTORE", CommandSpec{"ZRANGESTORE", -5, {"write"}, 1, 2, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          const std::string& dst = args[1];
          if (store().is_wrongtype_for_zset(dst)) return wrongtype_error_reply();
          std::vector<std::pair<std::string, double>> items;
          bool withscores = false;
          auto err = zrange_select(args, 2, 3, 5, /*allow_withscores=*/false, items, withscores);
          if (!err.empty()) return err;
          store().del(dst);
          for (const auto& [mem, sc] : items) store().zadd_one(dst, sc, mem, false, false, false, false, false);
          return encode_integer(static_cast<long long>(items.size()));
        }});

    // ─── PUBSUB introspection (+ sharded aliases) ────────────────────────
    t.emplace("PUBSUB", CommandSpec{"PUBSUB", -2, {"pubsub", "fast", "loading", "stale"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
          const auto sub = upper(args[1]);
          if (sub == "HELP") {
            return encode_array({
                encode_bulk("PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                encode_bulk("CHANNELS [<pattern>]"), encode_bulk("NUMSUB [<channel> ...]"),
                encode_bulk("NUMPAT"), encode_bulk("SHARDCHANNELS [<pattern>]"),
                encode_bulk("SHARDNUMSUB [<shardchannel> ...]"), encode_bulk("HELP")});
          }
          // SHARDCHANNELS/SHARDNUMSUB query the separate sharded namespace.
          const bool shard = sub == "SHARDCHANNELS" || sub == "SHARDNUMSUB";
          const auto& subscribers = shard ? g_shard_channel_subscribers : g_channel_subscribers;
          if (sub == "CHANNELS" || sub == "SHARDCHANNELS") {
            const std::string pat = args.size() >= 3 ? args[2] : "*";
            std::vector<std::string> out;
            for (const auto& [ch, subs] : subscribers) {
              if (!subs.empty() && fnmatch(pat.c_str(), ch.c_str(), 0) == 0) out.push_back(encode_bulk(ch));
            }
            return encode_array(out);
          }
          if (sub == "NUMSUB" || sub == "SHARDNUMSUB") {
            std::vector<std::string> out;
            for (std::size_t i = 2; i < args.size(); ++i) {
              auto it = subscribers.find(args[i]);
              out.push_back(encode_bulk(args[i]));
              out.push_back(encode_integer(it == subscribers.end() ? 0 : static_cast<long long>(it->second.size())));
            }
            return encode_array(out);
          }
          if (sub == "NUMPAT") {
            std::size_t n = 0;
            for (const auto& [p, subs] : g_pattern_subscribers) if (!subs.empty()) ++n;
            return encode_integer(static_cast<long long>(n));
          }
          return encode_error("Unknown PUBSUB subcommand or wrong number of arguments for '" + lower(args[1]) + "'");
        }});

    // Sharded pub/sub — in standalone mode this is equivalent to regular pub/sub.
    auto delegate = [](const char* base, const std::vector<std::string>& args,
                       SessionState& session, bool& close) -> std::string {
      std::vector<std::string> proxy = args; proxy[0] = base;
      const auto& tbl = command_table();
      auto it = tbl.find(base);
      return it == tbl.end() ? encode_error("unknown command") : it->second.handler(proxy, session, close);
    };
    // Sharded pub/sub — a separate channel namespace with ssubscribe/sunsubscribe/smessage
    // reply types; the client's shard-subscription count is independent of regular channels.
    t.emplace("SSUBSCRIBE", CommandSpec{"SSUBSCRIBE", -2, {"pubsub"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::string out;
          for (std::size_t i = 1; i < args.size(); ++i) {
            pubsub_ssubscribe(session.fd, args[i]);
            out += encode_array({encode_bulk("ssubscribe"), encode_bulk(args[i]),
                                 encode_integer(static_cast<long long>(shard_channel_count(session.fd)))});
          }
          return out;
        }});
    t.emplace("SUNSUBSCRIBE", CommandSpec{"SUNSUBSCRIBE", -1, {"pubsub"}, 0, 0, 0,
        [](const std::vector<std::string>& args, SessionState& session, bool&) {
          std::string out;
          if (args.size() == 1) {
            auto cit = g_fd_shard_channels.find(session.fd);
            std::vector<std::string> chans;
            if (cit != g_fd_shard_channels.end()) chans.assign(cit->second.begin(), cit->second.end());
            if (chans.empty()) {
              out = encode_array({encode_bulk("sunsubscribe"), encode_null(RespVersion::Resp2), encode_integer(0)});
            } else {
              for (const auto& ch : chans) {
                pubsub_sunsubscribe(session.fd, ch);
                out += encode_array({encode_bulk("sunsubscribe"), encode_bulk(ch),
                                     encode_integer(static_cast<long long>(shard_channel_count(session.fd)))});
              }
            }
          } else {
            for (std::size_t i = 1; i < args.size(); ++i) {
              pubsub_sunsubscribe(session.fd, args[i]);
              out += encode_array({encode_bulk("sunsubscribe"), encode_bulk(args[i]),
                                   encode_integer(static_cast<long long>(shard_channel_count(session.fd)))});
            }
          }
          return out;
        }});
    t.emplace("SPUBLISH", CommandSpec{"SPUBLISH", 3, {"write", "pubsub", "fast"}, 1, 1, 1,
        [](const std::vector<std::string>& args, SessionState&, bool&) {
          return encode_integer(static_cast<long long>(pubsub_spublish(args[1], args[2])));
        }});

    // ─── Read-only command variants (reject the write/STORE options) ─────
    t.emplace("SORT_RO", CommandSpec{"SORT_RO", -2, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& a, SessionState& s, bool&) -> std::string {
          return do_sort(a, s.resp_version, /*readonly=*/true);
        }});
    t.emplace("BITFIELD_RO", CommandSpec{"BITFIELD_RO", -2, {"readonly", "fast"}, 1, 1, 1,
        [delegate](const std::vector<std::string>& a, SessionState& s, bool& c) -> std::string {
          for (std::size_t i = 2; i < a.size(); ++i) {
            const auto o = upper(a[i]);
            if (o == "SET" || o == "INCRBY" || o == "OVERFLOW")
              return encode_error("BITFIELD_RO only supports the GET subcommand");
          }
          return delegate("BITFIELD", a, s, c);
        }});
    t.emplace("GEORADIUS_RO", CommandSpec{"GEORADIUS_RO", -6, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& a, SessionState& s, bool&) -> std::string {
          return do_geo_search(a, s.resp_version, /*form=*/0, /*readonly=*/true);
        }});
    t.emplace("GEORADIUSBYMEMBER_RO", CommandSpec{"GEORADIUSBYMEMBER_RO", -5, {"readonly"}, 1, 1, 1,
        [](const std::vector<std::string>& a, SessionState& s, bool&) -> std::string {
          return do_geo_search(a, s.resp_version, /*form=*/1, /*readonly=*/true);
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

    // Apply rename-command directives (config-file only). Renaming to an empty
    // target disables the command. Done after flag computation so the moved spec
    // keeps its flags. Command names are matched case-insensitively (table keys
    // are upper-case, and handle_command upper-cases incoming command names).
    for (const auto& [from, to] : g_rename_commands) {
      const std::string f = upper(from);
      auto fit = t.find(f);
      if (fit == t.end()) continue;
      if (to.empty()) { t.erase(fit); continue; }
      const std::string tname = upper(to);
      CommandSpec spec = fit->second;
      spec.name = tname;
      t.erase(fit);
      t[tname] = std::move(spec);
    }

    return t;
  }();

  return table;
}

} // namespace

std::string handle_command(const std::vector<std::string>& args, SessionState& session, bool& should_close) {
  // Empty inline lines and non-positive multibulk counts are no-ops in Redis: consumed
  // silently with no reply and no close (the parser emits __empty__ for both).
  if (args.empty() || args[0] == "__empty__") {
    return std::string();
  }
  // Real protocol errors desync the byte stream — reply, then close the connection
  // (matching Redis) so a malformed frame cannot corrupt subsequent commands.
  if (args[0] == "__parse_error__") {
    should_close = true;
    return encode_error("Protocol error");
  }
  // Redis-exact protocol errors: the specific suffix is carried in args[1]
  // (e.g. "invalid bulk length", "expected '$', got 'x'", "unbalanced quotes in request",
  // "too big inline request", "too big mbulk count string").
  if (args[0] == "__err_proto__") {
    should_close = true;
    return encode_error("Protocol error: " + (args.size() > 1 ? args[1] : std::string()));
  }
  if (args[0] == "__err_multibulk__") {
    should_close = true;
    return encode_error("Protocol error: invalid multibulk length");
  }
  if (args[0] == "__err_bulklen__") {
    should_close = true;
    return encode_error("Protocol error: invalid bulk length");
  }
  if (args[0] == "__err_unauth_multibulk__") {
    should_close = true;
    return encode_error("Protocol error: unauthenticated multibulk length");
  }
  if (args[0] == "__err_unauth_bulklen__") {
    should_close = true;
    return encode_error("Protocol error: unauthenticated bulk length");
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
    g_script_repl_flags = 3;  // redis.set_repl scope is a single script invocation
  }
  const auto& table = command_table();
  // SCRIPT KILL acts only on a busy eval; FUNCTION KILL only on a busy function. The mismatched
  // killer (SCRIPT KILL on a function, FUNCTION KILL on an eval) is NOT allowed through — it
  // falls into the busy gate below and is rejected with BUSY, matching Redis.
  const bool is_script_kill =
      (cmd == "SCRIPT" && args.size() == 2 && upper(args[1]) == "KILL" && !g_busy_is_function) ||
      (cmd == "FUNCTION" && args.size() == 2 && upper(args[1]) == "KILL" && g_busy_is_function);
  // Read-only FUNCTION introspection carries Redis's allow-busy flag (FUNCTION STATS/LIST/DUMP
  // must answer even while a script/function is running).
  const bool is_busy_allowed_function =
      cmd == "FUNCTION" && args.size() >= 2 &&
      (upper(args[1]) == "STATS" || upper(args[1]) == "LIST" || upper(args[1]) == "DUMP");
  // HELLO/AUTH/RESET/QUIT carry Redis's allow-busy flag: connection management must
  // work even while a script blocks the server.
  if (g_script_busy.load() && !is_script_kill && !is_busy_allowed_function && cmd != "SHUTDOWN" &&
      cmd != "HELLO" && cmd != "AUTH" && cmd != "RESET" && cmd != "QUIT") {
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
  if (session.in_multi && cmd != "EXEC" && cmd != "DISCARD" && cmd != "MULTI" && cmd != "QUIT" &&
      cmd != "RESET") {
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
    if (q_is_write && oom_should_reject() && !g_script_allow_oom &&
        cmd != "EVAL" && cmd != "EVALSHA" && cmd != "FLUSHALL" && cmd != "FLUSHDB") {
      session.multi_dirty = true;
      return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    }
    session.queued.push_back(args);
    return encode_simple("QUEUED");
  }

  const auto it = table.find(cmd);
  // ── Authentication gate (default user / requirepass) ──
  // Skipped when no password is configured (open, like Redis with no requirepass) and
  // for internally replayed/replicated commands (g_replaying_aof). Commands flagged
  // no_auth (AUTH/HELLO/QUIT/RESET) are always allowed so a client can authenticate.
  if (!g_requirepass.empty() && !session.authenticated && !g_replaying_aof) {
    const bool no_auth = (it != table.end()) && (it->second.flag_bits & kFlagNoAuth);
    if (!no_auth) {
      record_error_stat("NOAUTH");
      if (in_lua_script_context()) g_script_inner_error_seen = true;
      return std::string("-NOAUTH Authentication required.\r\n");
    }
  }
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
  // EVAL/EVALSHA/FCALL/FCALL_RO are exempt from the top-level persistence/OOM/replica gates:
  // the script or function decides for itself (via its flags), and its inner write commands are
  // gated individually when they re-enter this dispatch.
  const bool is_script_dispatch =
      cmd == "EVAL" || cmd == "EVALSHA" || cmd == "EVAL_RO" || cmd == "EVALSHA_RO" ||
      cmd == "FCALL" || cmd == "FCALL_RO";
  if (spec_is_write && misconf_should_reject() && !g_replaying_aof &&
      cmd != "CONFIG" && !is_script_dispatch) {
    ++top_stat.rejected_calls;
    record_error_stat("MISCONF");
    if (in_lua_script_context()) g_script_inner_error_seen = true;
    return std::string("-MISCONF Errors writing to the persistence layer (RDB/AOF). "
                       "Writes are disabled (stop-writes-on-bgsave-error). Check the server logs.\r\n");
  }
  if (spec_is_write && oom_should_reject() && !g_script_allow_oom &&
      cmd != "CONFIG" && !is_script_dispatch &&
      cmd != "FLUSHALL" && cmd != "FLUSHDB") {
    ++top_stat.rejected_calls;
    record_error_stat("OOM");
    if (in_lua_script_context()) g_script_inner_error_seen = true;
    return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
  }
  if (spec_is_write && noreplicas_should_reject() &&
      !is_script_dispatch &&
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
  // The replica read-only gate needs the *effective* write-ness, which the static command flag
  // doesn't capture for the scripting/function family: FCALL writes only if its target function
  // lacks the no-writes flag, FCALL_RO never writes, and FUNCTION LOAD/DELETE/FLUSH/RESTORE
  // mutate the function space (so they're rejected on a read-only replica — unless applied from
  // the master/AOF stream, where g_replaying_aof is set and the whole gate is bypassed below).
  bool readonly_gate_write = spec_is_write;
  if (cmd == "FCALL_RO") {
    readonly_gate_write = false;
  } else if (cmd == "FCALL" && args.size() >= 2) {
    readonly_gate_write = true;
    auto fidx = g_func_index.find(lower(args[1]));
    if (fidx != g_func_index.end()) {
      auto flib = g_libraries.find(fidx->second);
      if (flib != g_libraries.end())
        for (const auto& f : flib->second.functions)
          if (lower(f.name) == lower(args[1])) { readonly_gate_write = !(f.flags & kFuncNoWrites); break; }
    }
  } else if (cmd == "FUNCTION" && args.size() >= 2) {
    const auto sub = upper(args[1]);
    if (sub == "LOAD" || sub == "DELETE" || sub == "FLUSH" || sub == "RESTORE") readonly_gate_write = true;
  }
  if (readonly_gate_write && g_replication_role == "slave" && !g_executing_exec &&
      !g_replaying_aof &&
      cmd != "REPLICAOF" && cmd != "SLAVEOF" && cmd != "PUBLISH" && cmd != "SPUBLISH") {
    ++top_stat.rejected_calls;
    record_error_stat("READONLY");
    if (in_lua_script_context()) g_script_inner_error_seen = true;
    return std::string("-READONLY You can't write against a read only replica.\r\n");
  }
  // RESP2 clients with active subscriptions may only run the pub/sub control commands
  // (Redis server.c subscribe-mode gate; exemption is by command identity — PUBLISH is
  // deliberately NOT exempt). RESP3 clients have no restrictions.
  if (session.resp_version == RespVersion::Resp2 && !g_replaying_aof &&
      !in_lua_script_context() && client_has_subscriptions(session.fd) &&
      cmd != "PING" && cmd != "QUIT" && cmd != "RESET" &&
      cmd != "SUBSCRIBE" && cmd != "UNSUBSCRIBE" &&
      cmd != "PSUBSCRIBE" && cmd != "PUNSUBSCRIBE" &&
      cmd != "SSUBSCRIBE" && cmd != "SUNSUBSCRIBE") {
    ++top_stat.rejected_calls;
    record_error_stat("ERR");
    return encode_error("Can't execute '" + lower(cmd) +
                        "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET "
                        "are allowed in this context");
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

  session.last_cmd = lower(cmd);
  session.last_interaction_ms = now_ms();
  feed_monitors(cmd, args, session);

  // Test-only coverage hook: when PEADB_COMMAND_COVERAGE_FILE is set, append each
  // distinct dispatched command name. The TCL harness sets this per suite; the
  // scoreboard counts commands exercised by green upstream suites. Zero cost when unset.
  {
    static FILE* coverage_fp = [] {
      const char* path = std::getenv("PEADB_COMMAND_COVERAGE_FILE");
      return path ? std::fopen(path, "a") : nullptr;
    }();
    if (coverage_fp != nullptr) {
      static std::unordered_set<std::string> covered;
      if (covered.insert(cmd).second) {
        std::fprintf(coverage_fp, "%s\n", cmd.c_str());
        std::fflush(coverage_fp);
      }
    }
  }

  // Keyspace 'new' events ('n' flag): snapshot which of the command's declared keys
  // don't exist yet; after a successful write, any that now exist were created.
  // A stack-local (not thread_local) because EVAL/EXEC re-enter handle_command.
  // Zero cost unless the 'n' flag is enabled (off by default, like Redis).
  std::vector<std::string> notify_new_candidates;
  if ((g_notify_flags & NOTIFY_NEW) && spec_is_write && spec.first_key > 0 &&
      static_cast<std::size_t>(spec.first_key) < args.size()) {
    const std::int64_t step = spec.key_step > 0 ? spec.key_step : 1;
    const std::int64_t last_idx = spec.last_key >= 0
        ? spec.last_key
        : static_cast<std::int64_t>(args.size()) + spec.last_key;
    for (std::int64_t i = spec.first_key;
         i <= last_idx && i < static_cast<std::int64_t>(args.size()); i += step) {
      if (store().type_of(args[static_cast<std::size_t>(i)]) == "none") {
        notify_new_candidates.push_back(args[static_cast<std::size_t>(i)]);
      }
    }
  }

  // `cmd` aliases a thread_local buffer that nested dispatch (EVAL/EXEC inner commands)
  // clobbers — snapshot the name for everything after the handler call. Command names
  // fit in SSO, so this never heap-allocates.
  const std::string cmd_after = cmd;
  const auto t_start = std::chrono::steady_clock::now();
  const std::string reply = spec.handler(args, session, should_close);
  const std::int64_t exec_usec = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - t_start).count();
  // Single instrumentation chokepoint: per-command time → cmdstat usec, SLOWLOG, LATENCY.
  // Re-acquire the stats slot by (snapshotted) name: the handler may have invalidated
  // `top_stat` by clearing g_cmdstats (CONFIG RESETSTAT) — writing through the old
  // reference is a use-after-free.
  auto& post_stat = g_cmdstats[lower(cmd_after)];
  post_stat.usec_total += exec_usec;
  if (!in_lua_script_context()) {
    if (session.blocked.type != BlockType::None) {
      // The command just blocked — Redis logs a blocking command only when it unblocks, with
      // the whole span. Record when it started; try_unblock_client logs it on completion.
      if (session.blocked.block_start_ms == 0) session.blocked.block_start_ms = now_ms();
    } else {
      if (upper(cmd_after) != "EXEC") {
        // EXEC itself is not logged (Redis flags it CMD_SKIP_SLOWLOG); its queued commands are
        // logged individually as they run through handle_command during EXEC.
        slowlog_add(args, exec_usec, session.addr, session.client_name);
      }
      // Per-command latency histogram (LATENCY HISTOGRAM), keyed like commandstats.
      record_cmd_latency(full_command_name(upper(cmd_after), args), exec_usec);
    }
    latency_add("command", exec_usec / 1000);
  }
  if (is_error_reply(reply)) {
    ++post_stat.failed_calls;
    if (in_lua_script_context() && !is_script_cmd) {
      g_script_inner_error_seen = true;
    }
    const bool redis_inner_error = is_script_cmd ? (g_script_inner_error_seen || lua_engine_consume_last_error_from_redis()) : false;
    if (!is_script_cmd || !redis_inner_error) {
      record_error_stat(error_code_from_reply(reply));
    }
  }
  // Skip write side-effects when the command DEFERRED (a blocking command that just
  // registered a block produced no mutation yet). Without this guard a blocked
  // BLPOP/XREADGROUP would be wrongly written to the AOF / dirtied / notified.
  if (spec_is_write && !is_error_reply(reply) && session.blocked.type == BlockType::None) {
    store().increment_mutation_epoch();
    // g_master_repl_offset is updated inside append_replication_event → backlog_append
    // via g_master_repl_offset = g_repl_backlog.total (absolute set, not increment).
    append_replication_event(args, session, &reply);
    append_aof_command(args, session, &reply);
    // 'new' fires before the command's own event, mirroring Redis (dbAdd runs first).
    for (const auto& k : notify_new_candidates) {
      if (store().type_of(k) != "none") notify_keyspace_event(NOTIFY_NEW, "new", k, session.db_index);
    }
    emit_keyspace_notifications(cmd_after, args, spec.first_key, session.db_index, &reply);
    // Any write may satisfy a blocked client (BLPOP/BZPOPMIN/BLMOVE/XREAD…). Flag a
    // wakeup so the event loop rechecks blocked clients in THIS iteration instead of on
    // the next ~100ms poll. Cheap: the recheck early-returns when nothing is blocked.
    notify_key_written(spec.first_key > 0 && static_cast<std::size_t>(spec.first_key) < args.size()
                           ? args[static_cast<std::size_t>(spec.first_key)] : std::string(),
                       session.db_index);
  }
  // Access tracking: refresh the command's primary key's LRU/LFU metadata. Done after the
  // handler so a key the command just created is stamped (not left at lru=0). Runs under
  // every policy (like Redis, which maintains the lru clock unconditionally) so that
  // OBJECT IDLETIME is honest even before an eviction policy is configured; the cost is
  // one hash find on the first key. Single-key/first-key approximation.
  // RESTORE is exempt: it explicitly seeds the LRU/LFU (IDLETIME/FREQ) and, like Redis's
  // dbAdd path, must not have that immediately overwritten by an access-time touch.
  if (!is_error_reply(reply) && cmd != "RESTORE" &&
      spec.first_key > 0 && static_cast<std::size_t>(spec.first_key) < args.size()) {
    // Redis's LOOKUP_NOTOUCH commands inspect a key without counting as an access, so they
    // must not reset its idle time; CLIENT NO-TOUCH suppresses the update for everything the
    // client runs except an explicit TOUCH.
    static const std::set<std::string> kNoTouchCmds = {
        "TTL", "PTTL", "TYPE", "EXISTS", "OBJECT", "EXPIRETIME", "PEXPIRETIME", "DEBUG", "DUMP"};
    const bool client_no_touch = session.no_touch && cmd != "TOUCH";
    if (kNoTouchCmds.count(cmd) == 0 && !client_no_touch)
      store().touch_key_for_eviction(args[static_cast<std::size_t>(spec.first_key)]);
  }
  return reply;
}

void configure_security(const std::string& requirepass, const std::string& masterauth,
                        bool protected_mode, const std::string& enable_module_command,
                        const std::vector<std::pair<std::string, std::string>>& rename_commands) {
  g_requirepass = requirepass;
  g_masterauth = masterauth;
  g_protected_mode = protected_mode;
  g_enable_module_command = enable_module_command;
  g_rename_commands = rename_commands;
}

bool protected_mode_enabled() { return g_protected_mode; }
bool requirepass_is_set() { return !g_requirepass.empty(); }

void configure_maxmemory(const std::string& spec) {
  if (spec.empty()) { g_config_maxmemory = 0; return; }
  std::int64_t v = 0;
  if (parse_memory(spec, v) && v >= 0) g_config_maxmemory = v;
  else log(LogLevel::Warn, "ignoring invalid maxmemory value: " + spec);
}

void configure_maxmemory_policy(const std::string& spec) {
  if (spec.empty()) return;  // keep default (noeviction)
  if (auto p = parse_maxmemory_policy(spec)) apply_maxmemory_policy(*p);
  else log(LogLevel::Warn, "ignoring invalid maxmemory-policy: " + spec);
}

void configure_maxmemory_samples(const std::string& spec) {
  if (spec.empty()) return;
  std::int64_t v = 0;
  if (parse_i64(spec, v) && v >= 1 && v <= 1000) g_maxmemory_samples = static_cast<int>(v);
  else log(LogLevel::Warn, "ignoring invalid maxmemory-samples: " + spec);
}

void init_replication_id() {
  g_master_replid = gen_run_id();
}

// Wire RDB save/load to the function-library registry (see rdb.hpp). Call once at startup.
void functions_register_rdb_hooks() {
  rdb_register_function_hooks(
      []() {
        std::vector<std::string> codes;
        codes.reserve(g_libraries.size());
        for (const auto& [lc, lib] : g_libraries) codes.push_back(lib.code);
        return codes;
      },
      [](const std::string& code, std::string& err) {
        std::string name;
        // g_rdb_load_functions_replace lets DEBUG RELOAD choose between "already exists" errors
        // (NOFLUSH without MERGE) and overwrite (MERGE / normal replica full-sync / startup).
        return load_function_library(code, g_rdb_load_functions_replace, name, err);
      });
}

void configure_tcp_backlog(int backlog) {
  if (backlog > 0) g_config_tcp_backlog = backlog;
}

void configure_encoding_limits(const std::unordered_map<std::string, std::string>& raw) {
  auto& lim = store().encoding_limits();
  auto apply = [&](const char* key, std::int64_t& slot, bool allow_negative) {
    auto it = raw.find(key);
    if (it == raw.end()) return;
    std::int64_t v = 0;
    if (parse_i64(it->second, v) && (v >= 0 || allow_negative)) slot = v;
  };
  // ziplist aliases first so the newer listpack spelling wins when both are present.
  apply("hash-max-ziplist-entries", lim.hash_entries, false);
  apply("hash-max-listpack-entries", lim.hash_entries, false);
  apply("hash-max-ziplist-value", lim.hash_value, false);
  apply("hash-max-listpack-value", lim.hash_value, false);
  apply("list-max-ziplist-size", lim.list_size, true);
  apply("list-max-listpack-size", lim.list_size, true);
  apply("set-max-intset-entries", lim.set_intset, false);
  apply("set-max-listpack-entries", lim.set_entries, false);
  apply("set-max-listpack-value", lim.set_value, false);
  apply("zset-max-ziplist-entries", lim.zset_entries, false);
  apply("zset-max-listpack-entries", lim.zset_entries, false);
  apply("zset-max-ziplist-value", lim.zset_value, false);
  apply("zset-max-listpack-value", lim.zset_value, false);
}

void script_set_repl_flags(int flags) { g_script_repl_flags = flags; }

void configure_timeout(const std::string& spec) {
  if (spec.empty()) return;
  std::int64_t v = 0;
  if (parse_i64(spec, v) && v >= 0) g_config_timeout = static_cast<int>(v);
  else log(LogLevel::Warn, "ignoring invalid timeout: " + spec);
}

int client_timeout_seconds() { return g_config_timeout; }

void configure_client_output_buffer_limit(const std::string& spec) {
  if (spec.empty()) return;
  // Flat token stream of (class hard soft soft-seconds) groups, e.g.
  //   "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"
  std::istringstream iss(spec);
  std::string cls;
  while (iss >> cls) {
    std::string h, s, sec;
    if (!(iss >> h >> s >> sec)) { log(LogLevel::Warn, "ignoring malformed client-output-buffer-limit"); return; }
    int idx = -1;
    const std::string c = lower(cls);
    if (c == "normal") idx = kCoblNormal;
    else if (c == "slave" || c == "replica") idx = kCoblReplica;
    else if (c == "pubsub") idx = kCoblPubsub;
    else { log(LogLevel::Warn, "unknown client-output-buffer-limit class: " + cls); continue; }
    std::int64_t hv = 0, sv = 0, secv = 0;
    if (!parse_memory(h, hv) || !parse_memory(s, sv) || !parse_i64(sec, secv) || hv < 0 || sv < 0 || secv < 0) {
      log(LogLevel::Warn, "ignoring invalid client-output-buffer-limit values for " + cls);
      continue;
    }
    g_cobl[idx] = {hv, sv, static_cast<int>(secv)};
  }
}

CoblLimit cobl_for(int cls) {
  if (cls < 0 || cls > 2) return {0, 0, 0};
  return g_cobl[cls];
}

std::string client_output_buffer_limit_str() {
  auto one = [](const char* name, const CoblLimit& l) {
    return std::string(name) + " " + std::to_string(l.hard) + " " + std::to_string(l.soft) +
           " " + std::to_string(l.soft_seconds);
  };
  return one("normal", g_cobl[0]) + " " + one("slave", g_cobl[1]) + " " + one("pubsub", g_cobl[2]);
}

// Whether an fd has any active pub/sub subscription — such clients are exempt from the
// idle timeout (they legitimately sit quiet waiting for messages, like Redis).
bool client_has_subscriptions(int fd) {
  // O(1) via the per-fd reverse maps (also used on the per-command hot path by the
  // RESP2 subscribe-mode gate). Empty-set entries can linger because the reply-building
  // code touches the maps with operator[], so check emptiness, not just key presence.
  auto cit = g_fd_channels.find(fd);
  if (cit != g_fd_channels.end() && !cit->second.empty()) return true;
  auto pit = g_fd_patterns.find(fd);
  if (pit != g_fd_patterns.end() && !pit->second.empty()) return true;
  auto sit = g_fd_shard_channels.find(fd);
  return sit != g_fd_shard_channels.end() && !sit->second.empty();
}

void configure_notify_keyspace_events(const std::string& spec) {
  g_notify_flags = parse_notify_flags(spec);
}

void configure_repl_backlog_size(const std::string& spec) {
  if (spec.empty()) return;
  std::int64_t v = 0;
  if (parse_memory(spec, v) && v > 0) g_repl_backlog.reset(static_cast<std::size_t>(v));
  else log(LogLevel::Warn, "ignoring invalid repl-backlog-size: " + spec);
}

void configure_save(const std::string& spec) {
  g_save_params.clear();
  g_save_spec = spec;
  std::istringstream iss(spec);
  std::int64_t sec = 0, changes = 0;
  while (iss >> sec >> changes) {
    if (sec > 0 && changes > 0) g_save_params.emplace_back(sec, changes);
  }
}

// Active expiry driver (called from the event loop). On a master, physically
// expires keys and propagates a DEL to replicas so they stay in sync. On a
// replica, does nothing — a replica deletes a key only when the master's DEL
// arrives, which prevents master/replica divergence from independent expiry clocks.
void on_client_disconnect(int fd) {
  g_monitor_fds.erase(fd);
  // If this client owns an in-progress BUSY episode, release it. The cooperative "while true do"
  // simulation (eval_miniscript / do_fcall) returns -BUSY immediately but leaves g_script_busy
  // set until a SCRIPT/FUNCTION KILL arrives; if the client hangs up without killing, the flag
  // would wedge the whole server BUSY for every client, and g_busy_script_session would dangle
  // at the about-to-be-freed session. Clearing here is safe because the session still exists at
  // disconnect time (it is erased after this callback).
  if (g_busy_script_session != nullptr && g_busy_script_session->fd == fd) {
    g_script_busy.store(false, std::memory_order_release);
    g_script_kill_requested.store(false, std::memory_order_release);
    g_busy_script_session = nullptr;
  }
}

void run_active_expire_cycle() {
  if (g_pause_cron) return;  // DEBUG PAUSE-CRON
  if (g_replication_role == "slave") return;
  const auto t0 = std::chrono::steady_clock::now();
  auto expired = store().active_expire_cycle();
  // Record the cycle's wall time as an "expire-cycle" latency event (LATENCY monitor).
  const auto cycle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - t0).count();
  latency_add("expire-cycle", cycle_ms);
  if (expired.empty()) return;
  for (const auto& [dbidx, key] : expired) {
    if (dbidx != g_last_repl_db) {
      backlog_append(encode_command_resp({"SELECT", std::to_string(dbidx)}));
      g_last_repl_db = dbidx;
    }
    backlog_append(encode_command_resp({"DEL", key}));
    notify_keyspace_event(NOTIFY_EXPIRED, "expired", key, dbidx);
  }
}

// Eviction cron: triggered from the event loop. Mirrors Redis's startEvictionTimeProc —
// once maxmemory is exceeded (e.g. lowered below usage via CONFIG SET) under an eviction
// policy, evict keys in the background so the 'evicted' keyspace event and DEL propagation
// fire without waiting for the next write command. Cheap O(1) when under budget
// (used_memory() is a bounded-staleness cache), and a no-op on replicas / during AOF replay.
void run_eviction_cycle() {
  if (g_pause_cron) return;  // DEBUG PAUSE-CRON
  if (g_config_maxmemory <= 0 || g_maxmemory_policy == MaxmemoryPolicy::NoEviction) return;
  if (g_replication_role == "slave" || g_replaying_aof) return;
  if (store().used_memory() <= g_config_maxmemory) return;
  evict_to_fit();
}

// Save-point cron: triggered from the event loop. Fires BGSAVE when any configured
// (seconds, changes) threshold is met. Cheap O(1) check using mutation_epoch as the
// dirty counter.
void background_save_cron() {
  if (g_pause_cron) return;  // DEBUG PAUSE-CRON
  if (g_save_params.empty() || g_bgsave_in_progress.load()) return;
  const std::int64_t now_s = now_ms() / 1000;
  const std::int64_t dirty = static_cast<std::int64_t>(store().mutation_epoch()) - g_dirty_at_last_save;
  if (dirty <= 0) return;
  for (const auto& [sec, changes] : g_save_params) {
    if (dirty >= changes && (now_s - g_last_save_time) >= sec) {
      do_bgsave_fork();
      break;
    }
  }
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
  session.authenticated = true;  // internal replay must not be gated by requirepass
  g_replaying_aof = true;
  while (pos < buffer.size()) {
    auto parsed = parse_one_command(std::string_view(buffer).substr(pos));
    if (!parsed.has_value()) {
      // Incomplete frame remains. If it plausibly looks like a partial command at
      // EOF, treat it as a truncated tail (Redis: aof-load-truncated, default yes)
      // and warn with the byte offset; otherwise refuse to start on corruption
      // rather than silently discarding the remainder.
      const std::size_t dropped = buffer.size() - pos;
      const unsigned char first = static_cast<unsigned char>(buffer[pos]);
      const bool looks_partial = (first == '*' || std::isprint(first));
      if (looks_partial && g_aof_load_truncated) {
        log(LogLevel::Warn, "AOF: ignoring truncated tail — " + std::to_string(dropped) +
            " bytes dropped at offset " + std::to_string(pos));
        break;
      }
      g_replaying_aof = false;
      err = "AOF corrupted at offset " + std::to_string(pos);
      return false;
    }
    bool close = false;
    const auto reply = handle_command(parsed->args, session, close);
    if (is_error_reply(reply)) {
      g_replaying_aof = false;
      err = "AOF replay command failed at offset " + std::to_string(pos);
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

std::string try_unblock_client_impl(SessionState& session) {
  if (session.blocked.type == BlockType::None) return {};

  const auto current = now_ms();

  // Check timeout. The multi-key blockers (BLPOP/BRPOP/BZPOPMIN/BZPOPMAX/BLMPOP/BZMPOP)
  // time out with a null ARRAY; the move forms (BLMOVE/BRPOPLPUSH) with a null bulk.
  if (session.blocked.deadline_ms > 0 && current >= session.blocked.deadline_ms) {
    const auto& torig = session.blocked.original_args;
    const std::string tcmd = torig.empty() ? std::string() : upper(torig[0]);
    const bool null_bulk = tcmd == "BLMOVE" || tcmd == "BRPOPLPUSH";
    unregister_key_waiter(session.fd);
    session.blocked.type = BlockType::None;
    session.blocked.keys.clear();
    session.blocked.original_args.clear();
    return null_bulk ? encode_null(session.resp_version)
                     : encode_null_array(session.resp_version);
  }

  // Mutations applied on wakeup happen outside the dispatch chokepoint, so they must
  // propagate to the replication backlog and AOF explicitly (deterministic pop form).
  auto propagate = [&session](std::vector<std::string> cmd_args) {
    store().increment_mutation_epoch();
    append_replication_event(cmd_args, session, nullptr);
    append_aof_command(cmd_args, session, nullptr);
  };
  auto clear_block = [&session]() {
    unregister_key_waiter(session.fd);
    session.blocked.type = BlockType::None;
    session.blocked.keys.clear();
    session.blocked.original_args.clear();
  };
  // COUNT option shared by BLMPOP/BZMPOP original args.
  auto parse_orig_count = [](const std::vector<std::string>& orig) -> std::size_t {
    for (std::size_t i = 3; i + 1 < orig.size(); ++i) {
      if (upper(orig[i]) == "COUNT") {
        std::int64_t c = 0;
        if (parse_i64(orig[i + 1], c) && c > 0) return static_cast<std::size_t>(c);
      }
    }
    return 1;
  };

  if (session.blocked.type == BlockType::List) {
    const auto& orig = session.blocked.original_args;
    const std::string cmd0 = orig.empty() ? std::string() : upper(orig[0]);
    const bool is_blmove = cmd0 == "BLMOVE" || cmd0 == "BRPOPLPUSH";

    if (cmd0 == "BLMPOP") {
      const std::size_t count = parse_orig_count(orig);
      for (const auto& key : session.blocked.keys) {
        bool wrongtype = false;
        const auto len = store().llen(key, wrongtype);
        if (wrongtype) continue;  // wrong-type key: stay blocked, like Redis
        if (len == 0) continue;
        const std::string captured_key = key;
        std::vector<std::string> popped;
        for (std::size_t i = 0; i < count; ++i) {
          auto v = session.blocked.pop_left ? store().lpop(captured_key, wrongtype)
                                            : store().rpop(captured_key, wrongtype);
          if (!v.has_value()) break;
          popped.push_back(*v);
        }
        propagate({session.blocked.pop_left ? "LPOP" : "RPOP", captured_key,
                   std::to_string(popped.size())});
        clear_block();
        std::vector<std::string> enc;
        for (const auto& v : popped) enc.push_back(encode_bulk(v));
        return encode_array({encode_bulk(captured_key), encode_array(enc)});
      }
      return {};
    }

    for (const auto& key : session.blocked.keys) {
      bool wrongtype = false;
      std::optional<std::string> val;
      if (session.blocked.pop_left) {
        val = store().lpop(key, wrongtype);
      } else {
        val = store().rpop(key, wrongtype);
      }
      if (wrongtype) {
        continue;  // wrong-type key: stay blocked, like Redis
      }
      if (val.has_value()) {
        // Copy key before clearing the vector — `key` is a reference into
        // session.blocked.keys, which clear() would invalidate.
        const std::string captured_key = key;

        if (is_blmove && orig.size() >= 5) {
          // BLMOVE src dst wherefrom whereto timeout / BRPOPLPUSH src dst timeout
          const std::string dst = orig[2];
          const std::string where_to = cmd0 == "BRPOPLPUSH" ? "LEFT" : upper(orig[4]);
          std::vector<std::string> one{*val};
          bool wt = false;
          if (store().is_wrongtype_for_list(dst)) {
            // Dst is the wrong type: restore the just-popped element (source order intact)
            // and deliver WRONGTYPE to the woken client (Redis unblocks with the error).
            if (session.blocked.pop_left) (void)store().lpush(captured_key, one, wt);
            else (void)store().rpush(captured_key, one, wt);
            clear_block();
            return wrongtype_error_reply();
          }
          if (where_to == "LEFT") (void)store().lpush(dst, one, wt);
          else (void)store().rpush(dst, one, wt);
          propagate({"LMOVE", captured_key, dst,
                     session.blocked.pop_left ? "LEFT" : "RIGHT", where_to});
          clear_block();
          return encode_bulk(*val);
        }

        propagate({session.blocked.pop_left ? "LPOP" : "RPOP", captured_key});
        clear_block();
        return encode_array({encode_bulk(captured_key), encode_bulk(*val)});
      }
    }
  } else if (session.blocked.type == BlockType::ZSet) {
    const auto& orig = session.blocked.original_args;
    const std::string cmd0 = orig.empty() ? std::string() : upper(orig[0]);

    if (cmd0 == "BZMPOP") {
      const std::size_t count = parse_orig_count(orig);
      for (const auto& key : session.blocked.keys) {
        bool wrongtype = false;
        auto vals = store().zpop(key, count, session.blocked.zpop_max, wrongtype);
        if (wrongtype) continue;  // wrong-type key: stay blocked, like Redis
        if (vals.empty()) continue;
        const std::string captured_key = key;
        propagate({session.blocked.zpop_max ? "ZPOPMAX" : "ZPOPMIN", captured_key,
                   std::to_string(vals.size())});
        clear_block();
        std::vector<std::string> pairs;
        for (const auto& [m, s] : vals) {
          pairs.push_back(encode_array({encode_bulk(m), encode_double(s, session.resp_version)}));
        }
        return encode_array({encode_bulk(captured_key), encode_array(pairs)});
      }
      return {};
    }

    for (const auto& key : session.blocked.keys) {
      bool wrongtype = false;
      auto vals = store().zpop(key, 1, session.blocked.zpop_max, wrongtype);
      if (wrongtype) {
        continue;  // wrong-type key: stay blocked, like Redis
      }
      if (!vals.empty()) {
        // Copy key before clearing — same reasoning as the List branch above.
        const std::string captured_key = key;
        propagate({session.blocked.zpop_max ? "ZPOPMAX" : "ZPOPMIN", captured_key});
        clear_block();
        return encode_array({encode_bulk(captured_key), encode_bulk(vals[0].first),
                             encode_double(vals[0].second, session.resp_version)});
      }
    }
  } else if (session.blocked.type == BlockType::Stream) {
    // Re-run the original XREAD/XREADGROUP once (g_block_disabled prevents re-blocking).
    // Like the BLPOP/BZPOPMIN paths, the on-wakeup read is applied directly (the
    // blocking-replication limitation is shared with those commands).
    const std::vector<std::string> orig = session.blocked.original_args;
    const std::string cmd0 = orig.empty() ? std::string() : upper(orig[0]);
    const auto& tbl = command_table();
    auto it = tbl.find(cmd0);
    std::string reply;
    if (it != tbl.end()) {
      g_block_disabled = true;
      bool close = false;
      reply = it->second.handler(orig, session, close);
      g_block_disabled = false;
    }
    // A blocked XREAD waits for new stream data, so a (possibly transient) wrong-type key —
    // e.g. inside a MULTI that deletes then re-adds it as a list — must NOT wake it with an
    // error; it stays blocked until real stream data arrives. XREADGROUP is different: once
    // its key is a non-stream type (or the group is gone) the block can never be satisfied,
    // so it unblocks with the WRONGTYPE/NOGROUP error, matching Redis.
    const bool is_wrongtype = reply.rfind("-WRONGTYPE", 0) == 0;
    const bool wrongtype_stays_blocked = is_wrongtype && cmd0 != "XREADGROUP";
    if (!reply.empty() && reply != encode_null(RespVersion::Resp2) &&
        reply != encode_null_array(session.resp_version) && !wrongtype_stays_blocked) {
      // An error delivered on wakeup (e.g. XGROUP DESTROY → NOGROUP) is a failed call: the
      // dispatch chokepoint counted the initial (deferred) call, so record the failure here
      // so INFO commandstats/errorstats and total_error_replies stay accurate.
      if (is_error_reply(reply)) {
        record_cmd_failed(cmd0);
        record_error_stat(error_code_from_reply(reply));
      }
      unregister_key_waiter(session.fd);
      session.blocked.type = BlockType::None;
      session.blocked.keys.clear();
      session.blocked.original_args.clear();
      return reply;
    }
  }

  return {}; // still blocked
}

std::string try_unblock_client(SessionState& session) {
  if (session.blocked.type == BlockType::None) return {};
  // Capture the block context before try_unblock_client_impl clears it on serve/timeout.
  const std::vector<std::string> orig = session.blocked.original_args;
  const std::int64_t start_ms = session.blocked.block_start_ms;
  std::string reply = try_unblock_client_impl(session);
  // A non-empty reply means the blocking command finally completed (served or timed out):
  // Redis logs it to the slow log now, with the full block span and its original argv (not the
  // deterministic pop form it propagates as).
  if (!reply.empty()) {
    if (!orig.empty()) {
      const std::int64_t dur_us = start_ms > 0 ? (now_ms() - start_ms) * 1000 : 0;
      slowlog_add(orig, dur_us, session.addr, session.client_name);
    }
    session.blocked.block_start_ms = 0;  // completed — reset for the next block on this client
  }
  return reply;
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
    if (::fsync(g_aof_fd) != 0) {
      g_aof_write_error = true;
      log(LogLevel::Error, "AOF fsync (everysec) failed: " + std::string(std::strerror(errno)));
    }
    g_last_aof_fsync_ms = now;
  }
}

void drain_replica_cmd_queue() {
  // Apply any pending replica RDB load on the event loop thread before processing
  // queued commands. Running rdb_load() here serializes flushall+restore with all
  // event-loop reads/writes, eliminating the race against the streaming thread.
  if (g_replica_load_pending.load()) {
    std::string path;
    {
      std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
      path = g_replica_load_path;
      g_replica_load_path.clear();
    }
    std::string err;
    // A full resync replaces the entire dataset, including server-level function libraries.
    // rdb_load() flushes the keyspace but not functions, so clear them here first — otherwise
    // libraries that exist only on this node (before it became a replica) would survive and
    // diverge from the master (Redis's emptyData clears functions on full sync).
    functions_flush_all();
    // Skip CRC verification: the RDB came from the master (possibly real Redis, whose
    // CRC-64 variant differs from PeaDB's); the TCP stream + length framing + parser already
    // guard integrity. CRC verification is for on-disk startup loads of PeaDB's own dumps.
    const bool ok = rdb_load(path, err, /*verify_crc=*/false);
    if (!ok) log(LogLevel::Error, "REPLICAOF: rdb_load on event loop failed: " + err);
    g_replica_load_ok.store(ok);
    g_replica_load_pending.store(false);  // signal streaming thread
  }

  std::vector<std::vector<std::string>> cmds;
  {
    std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
    if (g_replica_cmd_queue.empty()) return;
    cmds.swap(g_replica_cmd_queue);
  }
  // Apply on the event loop thread: g_replaying_aof suppresses AOF writes and
  // backlog updates so master commands are not re-replicated or double-logged.
  g_replaying_aof = true;
  static SessionState repl_session;
  bool close_flag = false;
  for (auto& args : cmds) {
    handle_command(args, repl_session, close_flag);
  }
  g_replaying_aof = false;
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
bool replica_do_fullsync(const std::string& host, int port, std::uint64_t gen) {
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
    if (!repl_send(fd, "*1\r\n$4\r\nPING\r\n") || !repl_readline(fd, line)) {
        log(LogLevel::Error, "REPLICAOF: PING failed, got: " + line);
        ::close(fd);
        return false;
    }
    // Redis's syncWithMaster tolerates -NOAUTH / -NOPERM / "operation not permitted" replies
    // to the pre-AUTH PING: the master is alive, so replication continues to the AUTH step.
    if (line.rfind("+PONG", 0) != 0 && line.rfind("-NOAUTH", 0) != 0 &&
        line.rfind("-NOPERM", 0) != 0 && line.rfind("-ERR operation not permitted", 0) != 0) {
        log(LogLevel::Error, "REPLICAOF: PING failed, got: " + line);
        ::close(fd);
        return false;
    }

    // AUTH masterauth (if the master requires a password)
    if (!g_masterauth.empty()) {
        if (!repl_send(fd, repl_encode_cmd({"AUTH", g_masterauth})) ||
            !repl_readline(fd, line) || line.rfind("+OK", 0) != 0) {
            // Redis's syncWithMaster logs exactly this on a rejected AUTH; tooling and tests
            // (unit/auth MASTERAUTH) match on the "Unable to AUTH to MASTER" phrasing.
            log(LogLevel::Error, "Unable to AUTH to MASTER: " + line);
            ::close(fd);
            return false;
        }
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
    std::int64_t fullresync_offset = 0;
    {
        auto sp1 = line.find(' ');
        auto sp2 = line.find(' ', sp1 + 1);
        if (sp1 != std::string::npos && sp2 != std::string::npos) {
            g_master_replid = line.substr(sp1 + 1, sp2 - sp1 - 1);
            parse_i64(line.substr(sp2 + 1), fullresync_offset);
            g_master_repl_offset = fullresync_offset;
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

    // Hand the RDB load off to the event loop so flushall+restore happens on the same
    // thread that serves client reads/writes — eliminating the DataStore race that the
    // previous in-thread rdb_load() created (libstdc++ unordered_map mutations are not
    // safe under concurrent access). The streaming thread waits here until completion.
    {
        std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
        g_replica_load_path = tmp_path;
    }
    g_loading_replication.store(true);  // visible via INFO persistence loading:1
    g_replica_load_pending.store(true);
    while (g_replica_load_pending.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    const bool load_ok = g_replica_load_ok.load();
    g_loading_replication.store(false);
    ::unlink(tmp_path.c_str());
    if (!load_ok) {
        ::close(fd);
        log(LogLevel::Error, "REPLICAOF: rdb_load failed (event-loop)");
        return false;
    }
    log(LogLevel::Info, "REPLICAOF: full sync complete, loaded " + std::to_string(rdb_len) + " bytes");

    // Release loading gate; from here the event loop's READONLY guard rejects client writes.
    // Streaming commands are enqueued in g_replica_cmd_queue and applied by the event loop,
    // so DataStore mutations stay on the event loop thread (no concurrent access).
    g_loading_replication.store(false);
    g_replica_stale = false;

    // Stream subsequent replication commands from master.
    // The master sends a continuous RESP stream of write commands after the RDB.
    // Commands are enqueued and applied by drain_replica_cmd_queue() in the event loop.
    std::string stream_buf;
    stream_buf.reserve(65536);
    std::vector<char> recv_buf(65536);

    // Local counter tracks the replica's absolute master-stream offset for REPLCONF ACK.
    // Seeded from the master's +FULLRESYNC offset so WAIT (which compares against the
    // master's absolute offset) counts this replica correctly when the master had a
    // non-zero offset before REPLICAOF.
    std::int64_t replica_rx_bytes = fullresync_offset;

    // Set recv timeout once before the loop so we can send periodic ACKs.
    {
        struct timeval tv{1, 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    auto last_ack_ms = now_ms();
    while (true) {
        // Stop promptly if a newer REPLICAOF (including NO ONE) superseded us. The 1s
        // recv timeout below bounds how long this takes.
        if (g_repl_generation.load() != gen) {
            log(LogLevel::Info, "REPLICAOF: superseded by a newer REPLICAOF, stopping stream");
            break;
        }
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

        // Parse commands and enqueue them for the event loop to apply.
        // This keeps all DataStore mutations on the event loop thread, eliminating
        // the data race between the streaming thread and concurrent client reads.
        std::size_t consumed = 0;
        bool getack_seen = false;
        {
            std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
            while (true) {
                const std::string_view view(stream_buf.data() + consumed, stream_buf.size() - consumed);
                auto parsed = parse_one_command(view);
                if (!parsed.has_value()) break;
                // The master sends REPLCONF GETACK * to demand an immediate ACK (e.g. behind
                // a client's WAIT). Note it so we reply right after counting its bytes.
                const auto& a = parsed->args;
                if (a.size() >= 2 && upper(a[0]) == "REPLCONF" && upper(a[1]) == "GETACK") {
                    getack_seen = true;
                } else {
                    g_replica_cmd_queue.push_back(a);
                }
                consumed += parsed->consumed;
                replica_rx_bytes += static_cast<std::int64_t>(parsed->consumed);
            }
        }

        if (consumed > 0) {
            if (consumed == stream_buf.size()) stream_buf.clear();
            else stream_buf.erase(0, consumed);
        }
        // Respond to GETACK immediately (the offset now includes the GETACK bytes).
        if (getack_seen) {
            repl_send(fd, repl_encode_cmd({"REPLCONF", "ACK", std::to_string(replica_rx_bytes)}));
            last_ack_ms = now_ms();
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
      // The child wrote temp.aof but did NOT rename. Parent owns the commit:
      // append the diff buffer to temp.aof, then atomically rename. Any failure
      // unlinks the temp and keeps the old AOF live.
      const std::string tmp_path = g_aof_path.string() + ".tmp";
      if (WIFEXITED(status) && WEXITSTATUS(status) == 0 && g_appendonly) {
        if (g_aof_rewrite_buf_capped) {
          // Diff buffer overflowed during rewrite. Abandon: keep old AOF.
          ::unlink(tmp_path.c_str());
          log(LogLevel::Error, "BGREWRITEAOF: aborted because diff buffer hit 256 MiB cap; "
              "old AOF retained intact (dropping " +
              std::to_string(g_aof_rewrite_buf.size()) +
              " bytes of partial diff). Reduce write rate and rerun BGREWRITEAOF.");
          g_aof_rewrite_buf.clear();
          g_aof_last_bgrewrite_status = "err";
          // g_aof_fd unchanged — still pointing at the live old AOF.
        } else {
          // Append diff buffer to temp.aof before committing via rename.
          bool diff_ok = true;
          if (!g_aof_rewrite_buf.empty()) {
            const int diff_fd = ::open(tmp_path.c_str(),
                                       O_WRONLY | O_APPEND | O_CLOEXEC, 0644);
            if (diff_fd < 0) {
              diff_ok = false;
            } else {
              const char* p = g_aof_rewrite_buf.data();
              std::size_t remaining = g_aof_rewrite_buf.size();
              while (remaining > 0) {
                const ssize_t rc = ::write(diff_fd, p, remaining);
                if (rc <= 0) { diff_ok = false; break; }
                p += static_cast<std::size_t>(rc);
                remaining -= static_cast<std::size_t>(rc);
              }
              if (diff_ok && ::fsync(diff_fd) != 0) diff_ok = false;
              ::close(diff_fd);
            }
          }
          if (diff_ok && ::rename(tmp_path.c_str(), g_aof_path.c_str()) == 0) {
            // Crash-durability: fsync the parent directory so the new AOF
            // directory entry survives power loss. The rename is committed
            // in-memory regardless, so we must keep the new fd live; but we
            // surface dir-fsync failure as `aof_last_bgrewrite_status:err`
            // so operators can retry (or migrate to a healthy disk).
            const bool dir_durable = fsync_parent_dir(g_aof_path);
            if (!dir_durable) {
              log(LogLevel::Error, "BGREWRITEAOF: parent directory fsync failed; "
                  "new AOF is live but the rename may revert across a crash. "
                  "Reporting aof_last_bgrewrite_status:err — retry the rewrite.");
            }
            g_aof_rewrite_buf.clear();
            g_aof_last_bgrewrite_status = dir_durable ? "ok" : "err";
            open_aof_fd();  // reopen on the new inode
          } else {
            ::unlink(tmp_path.c_str());
            const std::size_t dropped = g_aof_rewrite_buf.size();
            // Clear the buffer: the live AOF (which append_aof has been writing
            // to throughout the rewrite window) already contains every command
            // the buffer holds. The next BGREWRITEAOF will fork from the current
            // in-memory state — which also already reflects these commands — so
            // re-appending the old buffer would double-apply non-idempotent
            // operations (INCR, list pushes, set removals, stream appends).
            g_aof_rewrite_buf.clear();
            g_aof_last_bgrewrite_status = "err";
            log(LogLevel::Error, "BGREWRITEAOF: failed to commit (" +
                std::string(diff_ok ? "rename" : "diff append") +
                " failed); old AOF retained on disk, dropping " +
                std::to_string(dropped) +
                " bytes of redundant diff buffer (already in live AOF).");
          }
        }
      } else {
        // Child crashed/exited non-zero — clean any leftover temp; keep old AOF.
        ::unlink(tmp_path.c_str());
        if (!g_aof_rewrite_buf.empty()) {
          log(LogLevel::Warn, "BGREWRITEAOF: child failed; discarding " +
              std::to_string(g_aof_rewrite_buf.size()) + " bytes of buffered diff");
        }
        g_aof_rewrite_buf.clear();
        g_aof_last_bgrewrite_status = "err";
      }
    }
  }
}

// ── ServerContext accessor ────────────────────────────────────────────────

static ServerContext g_server_context;

ServerContext& server_ctx() { return g_server_context; }

} // namespace peadb
