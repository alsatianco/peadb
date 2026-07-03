#pragma once

#include <atomic>
#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <unordered_map>
#include <functional>
#include <chrono>
#include <utility>

namespace peadb {

enum class RespVersion {
  Resp2 = 2,
  Resp3 = 3,
};

enum class BlockType {
  None = 0,
  List,
  ZSet,
  Stream,
};

struct BlockedState {
  BlockType type = BlockType::None;
  std::vector<std::string> keys;     // keys being watched
  bool pop_left = true;              // for list: BLPOP vs BRPOP
  bool zpop_max = false;             // for zset: BZPOPMIN vs BZPOPMAX
  std::int64_t deadline_ms = 0;      // 0 = infinite
  std::vector<std::string> original_args; // original command args for context
};

struct WaitState {
  std::int64_t target_offset = 0;   // master offset at WAIT issue time
  std::int64_t deadline_ms = 0;     // absolute deadline; 0 = no timeout
  std::int64_t min_replicas = 0;    // number of replicas to wait for
};

struct SessionState {
  RespVersion resp_version = RespVersion::Resp2;
  int db_index = 0;
  bool in_multi = false;
  bool multi_dirty = false;
  std::vector<std::vector<std::string>> queued;
  std::optional<std::uint64_t> watched_epoch;
  std::unordered_map<std::string, std::optional<std::string>> watched_digests;
  bool asking = false;
  bool replica_stream = false;
  std::size_t repl_index = 0;
  bool repl_send_select = false;
  std::int64_t repl_ack_offset = 0;
  BlockedState blocked;
  std::optional<WaitState> wait_state;
  // CLIENT command support
  std::uint64_t client_id = 0;
  std::string client_name;
  std::string lib_name;
  std::string lib_ver;
  int fd = -1;  // file descriptor for this connection
  // ── Authentication ──
  // authenticated stays false until AUTH/HELLO succeeds; when no requirepass is
  // configured the dispatch-time gate is skipped entirely, so the default is open
  // exactly like Redis with no password set.
  bool authenticated = false;
  std::string user = "default";
  // ── Introspection (CLIENT LIST / MONITOR / CLIENT KILL) ──
  std::string addr;                       // peer "ip:port", set at accept
  std::int64_t created_ms = 0;            // connection creation time
  std::int64_t last_interaction_ms = 0;   // last command time (for idle)
  std::string last_cmd;                   // last command name (lowercased)
  bool is_monitor = false;                // client issued MONITOR
};

std::string handle_command(const std::vector<std::string>& args, SessionState& session, bool& should_close);
std::size_t command_count();
std::vector<std::string> command_names();
void configure_persistence(const std::string& dir, const std::string& dbfilename, bool appendonly,
                           const std::string& appendfilename, const std::string& appendfsync);
bool load_aof_file(const std::string& path, std::string& err);
void aof_fsync_if_needed(std::int64_t now_ms);
void reap_background_children();
void configure_runtime_port(int port);
void configure_max_clients(int max_clients);
int configured_max_clients();
// ── Security configuration (wired from main.cpp at startup) ──
void configure_security(const std::string& requirepass, const std::string& masterauth,
                        bool protected_mode, const std::string& enable_module_command,
                        const std::vector<std::pair<std::string, std::string>>& rename_commands);
bool protected_mode_enabled();   // read by the server accept loop
bool requirepass_is_set();       // read by the server accept loop (protected-mode bypass)
void configure_maxmemory(const std::string& spec);   // "100mb"/bytes; "" = unlimited
void configure_maxmemory_policy(const std::string& spec);   // e.g. "allkeys-lru"; "" = noeviction
void configure_maxmemory_samples(const std::string& spec);  // eviction sample size; "" = 5
void init_replication_id();   // randomize the replication id at startup (call once in main)
void configure_timeout(const std::string& spec);   // idle-client timeout (seconds); "" = keep 0
void configure_tcp_backlog(int backlog);            // listen(2) backlog, for CONFIG GET reporting
void script_set_repl_flags(int flags);              // redis.set_repl() from the Lua engine (1=AOF, 2=replicas)
int client_timeout_seconds();                       // current idle timeout (0 = disabled)
bool client_has_subscriptions(int fd);              // exempt pub/sub clients from idle reaping

// client-output-buffer-limit: per-class output buffer caps. hard=0 / soft=0 means no limit.
enum CoblClass { kCoblNormal = 0, kCoblReplica = 1, kCoblPubsub = 2 };
struct CoblLimit { std::int64_t hard; std::int64_t soft; int soft_seconds; };
void configure_client_output_buffer_limit(const std::string& spec);  // "normal H S Sec slave ... pubsub ..."
CoblLimit cobl_for(int cls);                          // limits for a CoblClass
std::string client_output_buffer_limit_str();         // current config as a CONFIG GET string
void configure_save(const std::string& spec);        // "sec changes [sec changes ...]"
void configure_repl_backlog_size(const std::string& spec);  // e.g. "256mb" (startup only)
void configure_notify_keyspace_events(const std::string& spec);  // e.g. "KEA"
void background_save_cron();     // event-loop save-point check (triggers BGSAVE)
void run_active_expire_cycle();  // event-loop active expiry (master propagates DEL; replica no-op)
void set_active_expire_enabled(bool enabled);
bool active_expire_enabled();
std::size_t replication_event_count();
std::string replication_event_at(std::size_t idx);
void notify_key_written(const std::string& key, int db_index);
void register_key_waiter(const std::string& key, int fd);
void unregister_key_waiter(int fd);
bool has_pending_wakeups();
void clear_pending_wakeups();
std::string try_unblock_client(SessionState& session);
// Apply replication commands queued by the streaming thread onto the event loop thread.
void drain_replica_cmd_queue();

// ── Lua script context (set by lua_engine, read by blocking command handlers) ──
void set_lua_script_context(bool in_script);
bool in_lua_script_context();
void record_lua_script_rejected_write(const std::string& cmd);

// ── Shutdown request (set by SHUTDOWN command, checked by server loop) ──
void request_shutdown();
bool shutdown_requested();

// ── Server stats (set from server.cpp, read from command.cpp INFO handler) ──
extern std::atomic<int> g_connected_clients;
extern std::atomic<std::int64_t> g_total_connections_received;
extern std::atomic<std::int64_t> g_total_commands_processed;
extern std::atomic<std::int64_t> g_server_start_time;
// Callback: given a target offset, returns how many replicas have acked >= offset
extern std::function<int(std::int64_t)> g_count_synced_replicas;
// Callback: returns the number of currently-connected replica sessions (replica_stream=true).
extern std::function<int()> g_count_connected_replicas;
// Callback: returns the number of currently-blocked clients (BLPOP/BRPOP/etc.).
extern std::function<int()> g_blocked_clients_cb;
// Callback: close clients matching (by="ID"|"ADDR"|"ANY", val), excluding skip_fd.
// Returns the number closed. Wired by server.cpp (owns fd lifecycle).
extern std::function<int(const std::string& by, const std::string& val, int skip_fd)> g_client_kill_cb;
// Clear per-fd server-side registries (monitors, etc.) when a client disconnects.
void on_client_disconnect(int fd);

// ── Client registry (set from server.cpp, read by CLIENT LIST/INFO) ──
extern std::atomic<std::uint64_t> g_next_client_id;
// Callback: returns formatted CLIENT LIST output for all connections
extern std::function<std::string()> g_client_list_callback;

// ── Pub/Sub infrastructure ──
// Callback: enqueue a message string to a specific client fd's write buffer.
// Returns true if the client fd is still connected.
extern std::function<bool(int fd, const std::string& data)> g_pubsub_send;
// Register/unregister a client fd for a channel or pattern.
void pubsub_subscribe(int fd, const std::string& channel);
void pubsub_unsubscribe(int fd, const std::string& channel);
void pubsub_psubscribe(int fd, const std::string& pattern);
void pubsub_punsubscribe(int fd, const std::string& pattern);
void pubsub_unsubscribe_all(int fd);
// Publish a message; returns number of receivers.
std::int64_t pubsub_publish(const std::string& channel, const std::string& message);

} // namespace peadb
