#pragma once

#include <atomic>
#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <unordered_map>
#include <functional>
#include <chrono>

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
