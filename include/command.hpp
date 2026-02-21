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
};

std::string handle_command(const std::vector<std::string>& args, SessionState& session, bool& should_close);
void configure_persistence(const std::string& dir, const std::string& dbfilename, bool appendonly,
                           const std::string& appendfilename);
bool load_aof_file(const std::string& path, std::string& err);
void configure_runtime_port(int port);
void configure_max_clients(int max_clients);
int configured_max_clients();
void set_active_expire_enabled(bool enabled);
bool active_expire_enabled();
std::size_t replication_event_count();
std::string replication_event_at(std::size_t idx);
void notify_key_written(const std::string& key, int db_index);
std::string try_unblock_client(SessionState& session);

// ── Server stats (set from server.cpp, read from command.cpp INFO handler) ──
extern std::atomic<int> g_connected_clients;
extern std::atomic<std::int64_t> g_total_connections_received;
extern std::atomic<std::int64_t> g_total_commands_processed;
extern std::atomic<std::int64_t> g_server_start_time;
extern std::atomic<int> g_connected_replicas;
// Callback: given a target offset, returns how many replicas have acked >= offset
extern std::function<int(std::int64_t)> g_count_synced_replicas;

} // namespace peadb
