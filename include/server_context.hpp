#pragma once

#include <atomic>
#include <filesystem>
#include <functional>
#include <string>

namespace peadb {

// ── Replication state ──────────────────────────────────────────────────────

struct ReplicationState {
    std::string role = "master";              // "master" or "slave"
    std::string master_replid = "0000000000000000000000000000000000000001";
    std::int64_t master_repl_offset = 0;
    std::string master_host;
    int master_port = 0;
    bool replica_stale = false;
    std::atomic<bool> loading{false};
    std::int64_t min_replicas_to_write = 0;
};

// ── Persistence state ──────────────────────────────────────────────────────

struct PersistenceState {
    std::filesystem::path snapshot_path = "dump.rdb";
    std::filesystem::path aof_path = "appendonly.aof";
    bool appendonly = false;
    std::string appendfsync = "everysec";     // "always", "everysec", "no"
    int aof_fd = -1;
    bool aof_write_error = false;
    std::int64_t last_aof_fsync_ms = 0;
    std::int64_t last_save_time = 0;
    std::string last_bgsave_status = "ok";
    std::atomic<pid_t> bgsave_child_pid{0};
    std::atomic<bool> bgsave_in_progress{false};
    std::int64_t bgsave_start_time = 0;
    std::atomic<pid_t> bgrewrite_child_pid{0};
    std::atomic<bool> bgrewrite_in_progress{false};
};

// ── Server stats (extern-visible, set from server.cpp) ────────────────────

struct ServerStats {
    std::atomic<int> connected_clients{0};
    std::atomic<std::int64_t> total_connections_received{0};
    std::atomic<std::int64_t> total_commands_processed{0};
    std::atomic<std::int64_t> server_start_time{0};
    std::atomic<int> connected_replicas{0};
    std::atomic<std::uint64_t> next_client_id{1};
};

// ── Server callbacks (wired by server.cpp at startup) ─────────────────────

struct ServerCallbacks {
    std::function<int(std::int64_t)> count_synced_replicas;
    std::function<std::string()> client_list;
    std::function<bool(int fd, const std::string& data)> pubsub_send;
};

// ── Top-level aggregate ───────────────────────────────────────────────────

struct ServerContext {
    ReplicationState replication;
    PersistenceState persistence;
    ServerStats stats;
    ServerCallbacks callbacks;
};

// Returns the process-global ServerContext (defined in command.cpp).
ServerContext& server_ctx();

} // namespace peadb
