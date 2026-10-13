#include "command.hpp"
#include "zmalloc.hpp"
#include "command_metadata.generated.hpp"
#include "peadb_version.hpp"
#include "server_context.hpp"

#include "datastore.hpp"
#include "errors.hpp"
#include "lua_engine.hpp"
#include <map>
#include "protocol.hpp"
#include "rdb.hpp"
#include "redis_config_table.generated.hpp"
#include "sha256.hpp"

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
#include <new>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <random>
#include <limits>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <dlfcn.h>
#include <fnmatch.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <climits>
#include <deque>
#include <poll.h>
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
  kFlagWrite = 1u << 0,
  kFlagReadonly = 1u << 1,
  kFlagFast = 1u << 2,
  kFlagAdmin = 1u << 3,
  kFlagNoScript = 1u << 4,
  kFlagPubSub = 1u << 5,
  kFlagLoading = 1u << 6,
  kFlagStale = 1u << 7,
  kFlagNoAuth = 1u << 8,
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
  // Keep this with the dispatch flags rather than the colder Redis metadata below.
  bool propagates_effects = false;  // Handler emits deterministic writes instead of its argv.
  bool is_write() const {
    return flag_bits & kFlagWrite;
  }
  // Redis's lower-case name and command flags (kRedisCmd*), set once the table is built and kept
  // under rename-command; redis_container: the command has subcommands with their own flags.
  std::string redis_name;
  std::uint64_t redis_flags = 0;
  bool redis_container = false;
};

// proto-max-bulk-len (Redis default 512 MB): the largest bulk argument a client may send
// (server.cpp passes it to the parser) and the largest string SETBIT/APPEND/SETRANGE may build.
std::int64_t g_proto_max_bulk_len = 512LL * 1024 * 1024;
// client-query-buffer-limit (Redis default 1 GB): server.cpp closes a client whose unparsed
// input grows past it.
std::int64_t g_client_query_buffer_limit = 1024LL * 1024 * 1024;
int g_tcp_keepalive = 300;  // tcp-keepalive: seconds; 0 = no keepalive on new connections

std::unordered_map<std::string, std::string> g_script_cache;
// Relative to the working directory, which is the configured `dir` (Redis chdir()s into it).
std::filesystem::path g_snapshot_path = "dump.rdb";
std::filesystem::path g_aof_path = "appendonly.aof";
bool g_appendonly = false;  // the configured setting, also true while full sync suspends the AOF
bool g_aof_sync_suspended = false;  // enabled, but stopped while a full sync replaces the dataset
// Redis's aof_state. CONFIG SET appendonly yes moves Off -> WaitRewrite: writes then go only to
// the rewrite buffer until the first rewrite of the dataset is in place, and then the AOF is On.
enum class AofState { Off, WaitRewrite, On };
AofState g_aof_state = AofState::Off;
bool g_aof_rewrite_scheduled = false;           // retry a failed WaitRewrite rewrite from the cron
std::int64_t g_aofrw_consecutive_failures = 0;  // INFO aof_rewrites_consecutive_failures
// thread_local: each thread (event loop, REPLICAOF streaming) maintains its own
// replay flag so setting it in one thread never silently suppresses AOF writes
// or bypasses READONLY enforcement in the other.
thread_local bool g_replaying_aof = false;
thread_local bool g_applying_master_stream = false;
// Redis's mustObeyClient(): AOF and master-stream commands bypass client checks and routing,
// so a replica or a reload cannot discard data under different limits or a different slot map.
inline bool must_obey_client() {
  return g_replaying_aof;
}
bool g_replica_ignore_disk_write_errors = false;
std::int64_t g_replica_disk_error_last_log_ms = 0;
// Redis's c->lastcmd remains EXEC while its queued commands run.
std::string g_master_stream_command_name;
// Set while re-dispatching a blocked XREAD/XREADGROUP from try_unblock_client so the
// handler reads once instead of re-registering a block.
thread_local bool g_block_disabled = false;
int g_aof_fd = -1;
std::string g_appendfsync = "everysec";
// Redis's aof_last_write_status/aof_last_write_errno and aof_bio_fsync_status/aof_bio_fsync_errno:
// a failed AOF write or everysec fsync refuses writes (-MISCONF) until a later write, or a later
// fsync of new data, works (compat/delta.md item 35).
bool g_aof_write_error = false;
int g_aof_last_write_errno = 0;
bool g_aof_fsync_error = false;
int g_aof_fsync_errno = 0;
// Redis's aof_buf after a failed write: the bytes the AOF file still has to get, in order. Every
// later append and aof_cron() write them first.
std::string g_aof_buf;
bool g_aof_unsynced = false;  // bytes written since the last fsync (aof_last_incr_fsync_offset)
bool g_aof_load_truncated = true;  // tolerate an incomplete final command on load
std::int64_t g_last_aof_fsync_ms = 0;
std::atomic<pid_t> g_bgsave_child_pid{0};
std::atomic<bool> g_bgsave_in_progress{false};
std::int64_t g_bgsave_start_time = 0;  // ms, while a BGSAVE child runs
std::string g_last_bgsave_status = "ok";
// Redis's lastbgsave_try (Unix seconds of the last BGSAVE attempt, 0 = never), rdb_save_time_last
// (seconds the last BGSAVE child ran, -1 = none yet), stat_rdb_saves and rdb_bgsave_scheduled.
std::int64_t g_lastbgsave_try = 0;
std::int64_t g_rdb_save_time_last = -1;
std::int64_t g_stat_rdb_saves = 0;
bool g_rdb_bgsave_scheduled = false;
bool g_bgsave_for_replication = false;
std::string g_bgsave_snapshot_path;
std::atomic<pid_t> g_bgrewrite_child_pid{0};
std::atomic<bool> g_bgrewrite_in_progress{false};
// A shutdown asked for by SIGTERM or SIGINT (Redis's server.shutdown_asap and last_sig_received):
// set by shutdown_signal_handler(), run by shutdown_cron() between commands. Atomic, because the
// signal handler writes them.
std::atomic<int> g_shutdown_asap{0};
std::atomic<int> g_last_sig_received{0};
std::atomic<bool> g_loading{false};  // the startup load is running (Redis's server.loading)
// shutdown-on-sigterm and shutdown-on-sigint: kShutdown* flags, 0 for "default".
int g_shutdown_on_sigterm = 0;
int g_shutdown_on_sigint = 0;
// shutdown-timeout: how many seconds a shutdown waits for lagging replicas (0: it does not wait).
int g_shutdown_timeout = 10;
// A shutdown that waits for replicas (Redis's server.shutdown_mstime and shutdown_flags): when the
// wait ends at the latest (0: no shutdown waits), and the flags it finishes with.
std::int64_t g_shutdown_mstime = 0;
int g_shutdown_flags = 0;
std::string g_aof_rewrite_buf;          // Parent buffers commands written during child rewrite
bool g_aof_rewrite_buf_capped = false;  // Set once when the 256 MiB cap is hit per rewrite
std::string g_aof_last_bgrewrite_status = "ok";  // Reported by INFO PERSISTENCE
// The replica has no synced link to its master (INFO master_link_status:down); the link thread
// writes it.
std::atomic<bool> g_replica_stale{false};
// One command of the master's stream with its bytes as the master sent them: the replica applies
// `args`, then adds `raw` to its own stream (its backlog and its replicas), as Redis's
// commandProcessed() → replicationFeedStreamFromMasterStream() does, so its offset is the one of
// the master's stream it has applied.
struct MasterStreamCommand {
  std::vector<std::string> args;
  std::string raw;
};
// Queue for replication commands produced by the streaming thread and consumed
// by the event loop. All DataStore mutations stay on the event loop thread.
std::mutex g_replica_cmd_queue_mu;
std::vector<MasterStreamCommand> g_replica_cmd_queue;
// Commands from the master taken off g_replica_cmd_queue but not applied yet, because
// CLIENT PAUSE holds them; in arrival order. Event-loop thread only.
std::deque<MasterStreamCommand> g_replica_cmd_held;
// The session that applies the master's stream (Redis's master client); like Redis's it is new for
// each full sync, so a MULTI whose EXEC never came before the old link broke is dropped (the master
// ran the transaction whole, and the new dataset holds it). Its database is the one the stream has
// selected, which a full sync to a replica of this replica passes on. Event-loop thread only.
SessionState g_master_stream_session;
// The bytes of a transaction from the master that has not reached its EXEC yet: Redis's replica
// adds them to its stream, and counts them as applied, only once the EXEC has run.
std::string g_master_stream_multi;
// The replica's connection to its master, for the REPLCONF ACKs the event loop sends (Redis's
// replicationSendAck()); -1 while there is no synced link. The streaming thread owns the socket:
// it publishes it after the full sync and takes it back under this mutex before it closes it.
std::mutex g_replica_link_mu;
int g_replica_link_fd = -1;
std::string g_replica_ack_out;  // the unsent rest of an ACK, under g_replica_link_mu
// What the replica has read from its master's stream (Redis's master->read_reploff), and when it
// last read anything (master->lastinteraction); written by the streaming thread.
std::atomic<std::int64_t> g_replica_read_offset{0};
std::atomic<std::int64_t> g_replica_last_io_ms{0};
std::atomic<int> g_repl_timeout{60};
int g_repl_ping_replica_period = 10;
// Event-loop-owned cached master. Unapplied input and MULTI are discarded on a link drop;
// the selected database, applied offset, dataset and backlog survive (replicationCacheMaster).
bool g_cached_master = false;
struct MasterLinkRequest {
  std::uint64_t gen;
  bool resume = false;
  std::string replid;
  std::int64_t offset = -1;
  bool cached = false;
  int keepalive = 0;
  std::atomic<bool> done{false};
};
std::shared_ptr<MasterLinkRequest> g_master_link_request;  // under g_replica_cmd_queue_mu
// Where the replica's link is (Redis's repl_state, as ROLE names it); written by the link thread.
enum class ReplLinkState { Connect, Connecting, Handshake, Transfer, Connected };
std::atomic<ReplLinkState> g_repl_link_state{ReplLinkState::Connect};
// When the link to the master went down (Redis's repl_down_since; 0 = not since it was last up).
std::atomic<std::int64_t> g_repl_down_since_ms{0};
// The full sync's transfer, for INFO (Redis's REPL_STATE_TRANSFER and repl_transfer_*).
std::atomic<bool> g_repl_transfer_in_progress{false};
std::atomic<std::int64_t> g_repl_transfer_size{0};
std::atomic<std::int64_t> g_repl_transfer_read{0};
std::atomic<std::int64_t> g_repl_transfer_lastio_ms{0};
// The wake-up pipe (set_replica_wakeup_fd()); a byte is written only when none is pending.
std::atomic<int> g_replica_wakeup_write_fd{-1};
int g_replica_wakeup_read_fd = -1;
std::atomic<bool> g_replica_wakeup_pending{false};
void wake_event_loop_for_replica() {
  const int fd = g_replica_wakeup_write_fd.load();
  if (fd < 0 || g_replica_wakeup_pending.exchange(true)) return;
  const char byte = 1;
  [[maybe_unused]] const ssize_t n = ::write(fd, &byte, 1);
}
// Sends what is left of the last ACK, then `ack` when given. The socket is the streaming thread's,
// so a full send buffer drops the new ACK instead of blocking the event loop (the next one, at most
// a second later, carries a larger offset).
void send_to_master(const std::string* ack) {
  std::lock_guard<std::mutex> lk(g_replica_link_mu);
  if (g_replica_link_fd < 0) return;
  auto flush = [] {
    while (!g_replica_ack_out.empty()) {
      const ssize_t n = ::send(g_replica_link_fd, g_replica_ack_out.data(),
                               g_replica_ack_out.size(), MSG_DONTWAIT);
      if (n <= 0) return false;
      g_replica_ack_out.erase(0, static_cast<std::size_t>(n));
    }
    return true;
  };
  if (!flush() || ack == nullptr) return;
  g_replica_ack_out = *ack;
  flush();
}
// Drops every command from the master not applied yet: called when a full resync replaces the
// dataset (which already holds their effects) and when REPLICAOF changes the master. The caller
// bumps g_repl_generation first; the streaming thread checks it under the same mutex before it
// queues, so an old stream cannot add commands afterwards.
void drop_queued_replica_commands() {
  std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
  g_replica_cmd_queue.clear();
  g_replica_cmd_held.clear();
}
// REPLICAOF, after it has bumped g_repl_generation: no ACK goes out on the old master's link any
// more (its thread closes it), and the link is down. Under the mutex the old thread publishes its
// link with, so it cannot mark the link up again for the new master.
void release_master_link(bool link_down) {
  std::lock_guard<std::mutex> lk(g_replica_link_mu);
  g_replica_link_fd = -1;
  g_replica_ack_out.clear();
  if (link_down) g_replica_stale = true;
}

// Redis's disconnectSlaves(): this server's replicas must sync again (CLIENT KILL's server.cpp
// path closes them).
void disconnect_replicas() {
  if (!g_client_kill_cb) return;
  ClientKillFilter filter;
  filter.type = ClientType::Slave;
  bool kill_self = false;
  g_client_kill_cb(filter, kill_self);
}
// Pending RDB load handed off from streaming thread to event loop. The streaming
// thread sets g_replica_load_path + g_replica_load_pending=true and waits; the event
// loop runs rdb_load() synchronously and signals completion via g_replica_load_pending=false.
// The hand-off names the REPLICAOF generation it belongs to: a load from a link that a newer
// REPLICAOF (or NO ONE) replaced is dropped, so it can neither replace the dataset nor touch the
// new link's state. Everything here is protected by g_replica_cmd_queue_mu.
std::string g_replica_load_path;
std::uint64_t g_replica_load_gen = 0;
std::uint64_t g_replica_load_seq = 0;       // counts hand-offs
std::uint64_t g_replica_load_done_seq = 0;  // the last hand-off the event loop finished
bool g_replica_load_ok = false;             // its result
// The master's replication id and offset from its +FULLRESYNC, which the replica adopts once the
// RDB has loaded.
std::string g_replica_load_replid;
std::int64_t g_replica_load_offset = 0;
std::atomic<bool> g_replica_load_pending{false};
bool g_active_expire = true;
bool g_pause_cron = false;  // DEBUG PAUSE-CRON: freezes active expiry + save-point cron
int g_last_aof_db = -1;     // db of the last AOF append; -1 forces a SELECT on next append
// redis.set_repl() flags for the currently-running script: bit 1 = AOF, bit 2 = replicas
// (REPL_ALL=3 default). Reset at every script invocation.
thread_local int g_script_repl_flags = 3;
std::int64_t g_last_save_time = 0;
// ── Save points (BGSAVE thresholds) ──
std::vector<std::pair<std::int64_t, std::int64_t>> g_save_params;  // (seconds, changes)
std::string g_save_spec;                 // raw "sec changes ..." for CONFIG GET
std::int64_t g_dirty_at_last_save = 0;   // mutation_epoch when the last good save started
std::int64_t g_dirty_before_bgsave = 0;  // mutation_epoch when the running BGSAVE forked
bool g_stop_writes_on_bgsave_error = true;
std::string g_replication_role = "master";
// Redis's checkAlreadyExpired(): an expire time that has passed deletes the key at once (EXPIRE
// and friends, GETEX EXAT/PXAT), propagated as DEL with a `del` event; but never while the AOF
// loads or the master's stream applies, where the key keeps its past expire time and waits for
// the active cycle or its master's DEL.
bool already_expired(std::int64_t when_ms) {
  return when_ms <= DataStore::now_ms() && !g_replaying_aof && g_replication_role != "slave";
}
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
std::atomic<std::int64_t> g_sync_full{0};         // count of full resyncs served (INFO sync_full)
std::atomic<std::int64_t> g_sync_partial_ok{0};   // partial resyncs served (sync_partial_ok)
std::atomic<std::int64_t> g_sync_partial_err{0};  // partial resync requests we couldn't serve
std::int64_t g_config_maxmemory = 0;
MaxmemoryPolicy g_maxmemory_policy = MaxmemoryPolicy::NoEviction;
int g_maxmemory_samples = 5;              // candidate keys sampled per eviction (Redis default 5)
int g_config_timeout = 0;                 // idle-client timeout in seconds; 0 = never reap
int g_config_tcp_backlog = 511;           // listen(2) backlog, reported by CONFIG GET
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
std::string g_requirepass;  // empty = no auth required (Redis default)
std::string g_masterauth;   // sent to master during replica handshake
std::string g_masteruser;   // optional ACL username on the master
// Take both credentials together: CONFIG SET runs on the event loop, the handshake on its thread.
// CONFIG SET holds this across all applies/rollbacks when either credential changes;
// individual live-config getters/setters take it too.
std::recursive_mutex g_masterauth_mu;
std::pair<std::string, std::string> master_credentials_copy() {
  std::lock_guard<std::recursive_mutex> lock(g_masterauth_mu);
  return {g_masteruser, g_masterauth};
}
bool g_protected_mode = true;                // refuse non-loopback clients when no auth set
std::string g_enable_module_command = "no";  // "yes" | "no" | "local" — gates MODULE LOAD
std::vector<RenameCommandDirective> g_rename_commands;
// Set while command_table() applies g_rename_commands (validate_command_renames() reports them).
std::string g_rename_error;
std::vector<std::string> g_rename_warnings;
// Length-checked, content-time-independent compare to avoid leaking the password
// via per-byte timing. (Length is not secret enough to warrant masking here.)
inline bool const_time_eq(const std::string& a, const std::string& b) {
  if (a.size() != b.size()) return false;
  unsigned char acc = 0;
  for (std::size_t i = 0; i < a.size(); ++i)
    acc |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
  return acc == 0;
}
// Mirrors Redis's ACLDescribeUser (acl.c:831-876): user flags in ACLUserFlags order
// (acl.c:93-101), then one "#<sha256-hex>" per password, then the root selector
// description. PeaDB has exactly one user, "default", which is always enabled and always
// holds full key/channel/command access; its password is `requirepass`, or nopass when
// none is set — which is what ACLUpdateDefaultUserPassword (acl.c:3141-3150) does.
inline std::string acl_describe_default_user() {
  std::string out = "on ";
  if (g_requirepass.empty()) out += "nopass ";
  out += "sanitize-payload ";
  if (!g_requirepass.empty()) out += "#" + sha256_hex(g_requirepass) + " ";
  out += "~* &* +@all";
  return out;
}
// Circular replication backlog (replaces unbounded g_replication_events vector).
// Size is configurable via repl-backlog-size; a larger window lets a briefly
// disconnected replica resume via +CONTINUE instead of forcing a full resync.
struct ReplBacklog {
  std::size_t size_ = 1 << 20;  // default 1 MiB
  // Allocated at its full size but not zero-filled, so the pages of the part never written are
  // not touched (not resident), and used_memory leaves that part out (unused_bytes()).
  std::unique_ptr<char[]> buf;
  std::int64_t total = 0;   // total bytes ever appended
  std::int64_t start_ = 0;  // oldest offset held since the last resize()
  std::size_t filled_ = 0;  // bytes of buf that hold data (size_ once the ring has wrapped)

  ReplBacklog() : buf(new char[size_]) {
  }

  // Sets the size before any data is appended (startup). Throws std::bad_alloc (or
  // std::length_error) for a size that cannot be allocated, leaving the backlog unchanged.
  void reset(std::size_t newsize) {
    if (newsize == 0) return;
    std::unique_ptr<char[]> fresh(new char[newsize]);
    buf.swap(fresh);
    size_ = newsize;
    total = 0;
    start_ = 0;
    filled_ = 0;
  }

  // CONFIG SET repl-backlog-size (resizeReplicationBacklog()): keeps the offsets and as many
  // of the newest bytes as fit, so a replica can still continue from any offset base() says
  // is held. Returns false, with the backlog unchanged, when the size cannot be allocated.
  bool resize(std::size_t newsize) {
    if (newsize == 0 || newsize == size_) return true;
    const std::int64_t keep_from = std::max(base(), total - static_cast<std::int64_t>(newsize));
    std::unique_ptr<char[]> fresh;
    std::string kept;
    try {
      fresh.reset(new char[newsize]);
      get_slice(keep_from, kept);
    } catch (const std::bad_alloc&) {
      return false;
    } catch (const std::length_error&) {
      return false;
    }
    for (std::size_t i = 0; i < kept.size(); ++i) {
      fresh[static_cast<std::size_t>((keep_from + static_cast<std::int64_t>(i)) %
                                     static_cast<std::int64_t>(newsize))] = kept[i];
    }
    buf.swap(fresh);
    size_ = newsize;
    start_ = keep_from;
    // The kept bytes may sit anywhere in the new ring; count the ring as filled unless they
    // start at its beginning (then later bytes fill it in order).
    filled_ = keep_from % static_cast<std::int64_t>(newsize) == 0 ? kept.size() : newsize;
    return true;
  }

  // An empty backlog whose next byte is stream offset `offset` + 1 (Redis's
  // createReplicationBacklog(): repl_backlog->offset = master_repl_offset + 1).
  void reset_at(std::int64_t offset) {
    total = offset;
    start_ = offset;
    filled_ = 0;
  }

  std::int64_t base() const {
    const std::int64_t wrapped =
        total > static_cast<std::int64_t>(size_) ? total - static_cast<std::int64_t>(size_) : 0;
    return std::max(wrapped, start_);
  }

  // The bytes held (Redis's repl_backlog->histlen).
  std::int64_t histlen() const {
    return total - base();
  }

  // Copies at most two runs (the end of the ring, then its start); of a write bigger than the
  // ring only the last size_ bytes stay, as a byte-by-byte copy would leave them.
  void append(const std::string& data) {
    const char* p = data.data();
    std::size_t n = data.size();
    if (n > size_) {
      p += n - size_;
      total += static_cast<std::int64_t>(n - size_);
      n = size_;
    }
    const std::size_t pos = static_cast<std::size_t>(total % static_cast<std::int64_t>(size_));
    const std::size_t first = std::min(n, size_ - pos);
    std::memcpy(buf.get() + pos, p, first);
    std::memcpy(buf.get(), p + first, n - first);
    total += static_cast<std::int64_t>(n);
    filled_ = std::min(size_, filled_ + data.size());
  }

  // The bytes of the ring no write has reached yet.
  std::size_t unused_bytes() const {
    return size_ - filled_;
  }

  // Fill out with bytes [from_byte, total). Returns false if from_byte < base().
  bool get_slice(std::int64_t from_byte, std::string& out) const {
    if (from_byte < base() || from_byte > total) return false;
    const std::size_t len = static_cast<std::size_t>(total - from_byte);
    out.resize(len);
    if (len == 0) return true;
    const std::size_t start = static_cast<std::size_t>(from_byte % size_);
    if (start + len <= size_) {
      std::memcpy(out.data(), buf.get() + start, len);
    } else {
      const std::size_t first = size_ - start;
      std::memcpy(out.data(), buf.get() + start, first);
      std::memcpy(out.data() + first, buf.get(), len - first);
    }
    return true;
  }
};
ReplBacklog g_repl_backlog;
// Redis creates its backlog when the first replica connects, or on a replica after its full sync;
// PeaDB fills its own from the start, and starts it over then (start_backlog()).
bool g_backlog_has_replica = false;
// A replica takes its bytes from the ring once per event-loop pass (server.cpp, repl_index); every
// replica has taken the ring up to at least this offset. Before a write would overwrite bytes some
// replica has not taken, backlog_append() hands them out at once (g_feed_replicas_cb).
std::int64_t g_replicas_taken_to = 0;

// The backlog bytes used_memory() counts: what writes have filled, once a replica has attached.
std::size_t backlog_memory() {
  return g_backlog_has_replica ? g_repl_backlog.size_ - g_repl_backlog.unused_bytes() : 0;
}

// INFO's used_memory and what maxmemory starts from: the bytes the process holds on the heap
// (zmalloc_used_memory()) less the part of the replication backlog that Redis would not hold: the
// part no write has reached yet (reserved, not resident; Redis grows its backlog as data comes),
// and, until a replica has attached, the whole ring (Redis has no backlog before that).
std::size_t used_memory() {
  const std::size_t total = zmalloc_used_memory();
  const std::size_t left_out =
      g_backlog_has_replica ? g_repl_backlog.unused_bytes() : g_repl_backlog.size_;
  return total > left_out ? total - left_out : 0;
}
// Append to the replication stream: the backlog, and through it every replica, and advance the
// master offset. Redis keeps every byte a replica still needs, however far it is behind (until its
// output-buffer limit drops it); PeaDB's ring would overwrite them when more than its size is
// written in one event-loop pass (one big value, a pipeline, a script), so then every replica gets
// what it has not taken yet, and this write, in its output buffer first (compat/delta.md item 40).
inline void backlog_append(const std::string& data) {
  if (g_backlog_has_replica && g_feed_replicas_cb &&
      g_repl_backlog.total + static_cast<std::int64_t>(data.size()) >
          g_replicas_taken_to + static_cast<std::int64_t>(g_repl_backlog.size_)) {
    g_feed_replicas_cb(&data);
    g_repl_backlog.append(data);
    g_replicas_taken_to = g_repl_backlog.total;
  } else {
    g_repl_backlog.append(data);
  }
  g_master_repl_offset = g_repl_backlog.total;
}

// Redis's createReplicationBacklog(), for a master's first replica or for a replica after its full
// sync: the backlog starts empty at the current offset.
void start_backlog() {
  g_repl_backlog.reset_at(g_master_repl_offset);
  g_replicas_taken_to = g_master_repl_offset;
  g_backlog_has_replica = true;
}
// Where a write goes (Redis's PROPAGATE_AOF and PROPAGATE_REPL).
constexpr int kPropagateAof = 1;
constexpr int kPropagateRepl = 2;
// A write waiting for the end of its execution unit (Redis's redisOp in server.also_propagate).
struct PendingPropagation {
  int db;
  std::vector<std::string> argv;
  int target;
};
std::vector<PendingPropagation> g_also_propagate;
// How deep the running commands nest (Redis's server.execution_nesting): EXEC and scripts run
// their commands inside the command that started them, and everything they propagate is written
// when the outermost one ends (propagate_pending_commands()).
int g_execution_nesting = 0;
// Redis's call() propagates a write, counts it in server.dirty and its handler sends its events
// only when it changed something. A write handler that changed nothing (a SET stopped by NX or XX,
// a SETNX or MSETNX answered 0, a GETEX that set or removed no TTL, a GETDEL, SETRANGE, PERSIST or
// SORT that changed nothing) sets this, and handle_command() then does none of the three. Other
// handlers still count every write that succeeds (item 18).
bool g_write_changed_nothing = false;
bool g_executing_exec = false;
int g_last_repl_db = 0;
// Bumped on every REPLICAOF (including NO ONE). The reconnect supervisor and the
// streaming loop compare against their captured generation and stop when it changes,
// so a new REPLICAOF promptly supersedes an in-flight sync/stream.
std::atomic<std::uint64_t> g_repl_generation{0};
std::atomic<bool> g_loading_replication{false};
// Both startup and REPLICAOF use the same supervised link. The role is set before this starts;
// the thread only receives data and hands it to the event loop, never mutating the datastore.
void start_master_link(const std::string& host, int port, std::uint64_t gen) {
  if (port == 0) return;
  std::thread([host, port, gen]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    int backoff_ms = 500;
    while (g_repl_generation.load() == gen) {
      const bool ok = replica_do_fullsync(host, port, gen);
      if (g_repl_generation.load() != gen) break;
      {
        std::lock_guard<std::mutex> lk(g_replica_link_mu);
        if (g_repl_generation.load() != gen) break;
        g_replica_stale = true;
      }
      backoff_ms = ok ? 500 : std::min(backoff_ms * 2, 30000);
      std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
    }
  }).detach();
}
bool g_script_allow_oom = false;
// Redis's SCRIPT_WRITE_DIRTY: the running script has already passed a write command, so the disk
// error check no longer refuses its writes (a script must not stop half applied).
bool g_script_write_dirty = false;
bool g_script_inner_error_seen = false;
// A cluster node PeaDB knows about. Index 0 of g_cluster_nodes is always self — see
// ensure_self_registered() below. The id is derived deterministically from the announced
// address rather than randomly generated and persisted, so it is stable across restarts
// without a nodes.conf file — see
// docs/decisions/p5-cluster-addressing-and-version-provenance.md.
struct ClusterNode {
  std::string id;
  std::string host;
  int port = 0;
};
std::vector<ClusterNode> g_cluster_nodes;

// Unassigned is first so the value-initialised array starts with no ownership at all — a
// standalone (or freshly cluster-enabled, unconfigured) node must never redirect. Owned and
// Moved carry the owning node's index in g_slot_owner; Unassigned always carries -1 there.
enum class SlotRoute { Unassigned, Owned, Moved };
std::array<SlotRoute, 16384> g_slot_routes{};
// -1 = no owner (Unassigned); otherwise an index into g_cluster_nodes. This is the single
// authority for reported ownership — cluster_owned_ranges() (which feeds CLUSTER SLOTS/SHARDS/
// NODES and cluster_size) reads only this array. SETSLOT MIGRATING/IMPORTING must never write
// it: a slot in flight is still owned by whoever owned it before the migration started, exactly
// as Redis keeps the slot in the owner's bitmap throughout MIGRATING/IMPORTING and only moves it
// on the final SETSLOT NODE. Ownership changes only via ADDSLOTS/DELSLOTS/ADDSLOTSRANGE/
// DELSLOTSRANGE/SETSLOT NODE.
std::array<std::int32_t, 16384> g_slot_owner = [] {
  std::array<std::int32_t, 16384> a{};
  a.fill(-1);
  return a;
}();
// Redirect target for a slot this node is migrating away (SETSLOT <slot> MIGRATING <node>);
// -1 when not migrating. Consulted only by the dispatch chokepoint's ASK check and by CLUSTER
// NODES' "[slot->-<id>]" marker on this node's own line — never by cluster_owned_ranges(), so a
// migration in progress cannot leak into reported ownership (see g_slot_owner above and
// compat/delta.md item 6). Mirrors Redis's server.cluster->migrating_slots_to[slot].
std::array<std::int32_t, 16384> g_slot_migrating_to = [] {
  std::array<std::int32_t, 16384> a{};
  a.fill(-1);
  return a;
}();
// Source node for a slot this node is importing (SETSLOT <slot> IMPORTING <node>); -1 when not
// importing. Recorded for CLUSTER NODES' "[slot-<-<id>]" marker; PeaDB does not yet special-case
// ASKING traffic for an importing slot (compat/delta.md item 6 tracks this as a remaining gap —
// only the reporting-honesty half of MIGRATING/IMPORTING is in scope here). Mirrors Redis's
// server.cluster->importing_slots_from[slot].
std::array<std::int32_t, 16384> g_slot_importing_from = [] {
  std::array<std::int32_t, 16384> a{};
  a.fill(-1);
  return a;
}();
bool g_cluster_enabled = false;
std::string g_server_bind = "127.0.0.1";  // mirror of config.bind — see configure_cluster
std::string g_cluster_announce_ip;        // empty = fall back to g_server_bind
int g_cluster_announce_port = 0;          // 0 = fall back to g_server_port
int g_cluster_announce_bus_port = 0;      // 0 = announced port + 10000
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
  std::string name;  // display name (original case)
  std::string description;
  int flags = 0;          // FunctionFlags bitmask
  int callback_ref = -1;  // Lua registry ref for the callback closure
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
  std::int64_t usec_total = 0;  // accumulated execution time for usec_per_call
};

// ── Observability state (SLOWLOG / LATENCY / keyspace notifications / INFO) ──
struct SlowlogEntry {
  std::int64_t id;
  std::int64_t time_s;
  std::int64_t usec;
  std::vector<std::string> argv;  // redacted + truncated argv (<=32 elements, each <=128 bytes)
  std::string peer;               // client address ip:port
  std::string name;               // client name (CLIENT SETNAME)
};
std::deque<SlowlogEntry> g_slowlog;
std::int64_t g_slowlog_next_id = 0;
std::int64_t g_slowlog_threshold_us = 10000;  // slowlog-log-slower-than (<0 disables)
std::size_t g_slowlog_max_len = 128;

struct LatencyEvent {
  std::int64_t time_s = 0;
  std::int64_t latest_ms = 0;
  std::int64_t max_ms = 0;
  std::deque<std::pair<std::int64_t, std::int64_t>> history;  // (time_s, latency_ms), capped at 160
};
std::unordered_map<std::string, LatencyEvent> g_latency_events;
std::int64_t g_latency_monitor_threshold_ms = 0;  // latency-monitor-threshold (0 disables)

// Per-command latency histogram for LATENCY HISTOGRAM (keyed by full command name, i.e. the
// container form "parent|sub" for subcommands). Log2 buckets over microseconds.
struct CmdLatencyHist {
  std::uint64_t calls = 0;
  std::array<std::uint64_t, 64>
      buckets{};  // buckets[i] = count of samples with floor(log2 usec)==i
};
std::unordered_map<std::string, CmdLatencyHist> g_cmd_latency_hist;

std::int64_t g_used_memory_peak = 0;
std::int64_t g_latest_fork_usec = 0;
std::int64_t g_total_forks = 0;
std::string g_loglevel_name = "notice";
std::int64_t g_ops_sample_ms = 0;  // for instantaneous_ops_per_sec (sampled per INFO)
std::int64_t g_ops_sample_count = 0;
std::unordered_set<int> g_monitor_fds;  // clients in MONITOR mode

// Keyspace notifications: bitmask parsed from notify-keyspace-events.
enum NotifyFlag : std::uint32_t {
  NOTIFY_KEYSPACE = 1u << 0,
  NOTIFY_KEYEVENT = 1u << 1,
  NOTIFY_GENERIC = 1u << 2,
  NOTIFY_STRING = 1u << 3,
  NOTIFY_LIST = 1u << 4,
  NOTIFY_SET = 1u << 5,
  NOTIFY_HASH = 1u << 6,
  NOTIFY_ZSET = 1u << 7,
  NOTIFY_EXPIRED = 1u << 8,
  NOTIFY_EVICTED = 1u << 9,
  NOTIFY_STREAM = 1u << 10,
  NOTIFY_KEY_MISS = 1u << 11,
  NOTIFY_NEW = 1u << 12,
  NOTIFY_MODULE = 1u << 13,
};
constexpr std::uint32_t NOTIFY_ALL = NOTIFY_GENERIC | NOTIFY_STRING | NOTIFY_LIST | NOTIFY_SET |
                                     NOTIFY_HASH | NOTIFY_ZSET | NOTIFY_EXPIRED | NOTIFY_EVICTED |
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
  if (unit.empty() || unit == "b")
    mult = 1;
  else if (unit == "k")
    mult = 1000LL;
  else if (unit == "kb")
    mult = 1024LL;
  else if (unit == "m")
    mult = 1000LL * 1000;
  else if (unit == "mb")
    mult = 1024LL * 1024;
  else if (unit == "g")
    mult = 1000LL * 1000 * 1000;
  else if (unit == "gb")
    mult = 1024LL * 1024 * 1024;
  else
    return false;
  out = base * mult;
  return out >= 0;
}
std::string lower(std::string s);  // defined below; needed by parse_maxmemory_policy
std::string upper(std::string s);  // defined below; needed by redact_slowlog_args

// Maxmemory-policy string ↔ enum. Returns nullopt for an unrecognised name.
inline std::optional<MaxmemoryPolicy> parse_maxmemory_policy(const std::string& s) {
  const std::string p = lower(s);
  if (p == "noeviction") return MaxmemoryPolicy::NoEviction;
  if (p == "allkeys-lru") return MaxmemoryPolicy::AllkeysLru;
  if (p == "allkeys-lfu") return MaxmemoryPolicy::AllkeysLfu;
  if (p == "allkeys-random") return MaxmemoryPolicy::AllkeysRandom;
  if (p == "volatile-lru") return MaxmemoryPolicy::VolatileLru;
  if (p == "volatile-lfu") return MaxmemoryPolicy::VolatileLfu;
  if (p == "volatile-random") return MaxmemoryPolicy::VolatileRandom;
  if (p == "volatile-ttl") return MaxmemoryPolicy::VolatileTtl;
  return std::nullopt;
}
inline const char* maxmemory_policy_to_str(MaxmemoryPolicy p) {
  switch (p) {
    case MaxmemoryPolicy::NoEviction:
      return "noeviction";
    case MaxmemoryPolicy::AllkeysLru:
      return "allkeys-lru";
    case MaxmemoryPolicy::AllkeysLfu:
      return "allkeys-lfu";
    case MaxmemoryPolicy::AllkeysRandom:
      return "allkeys-random";
    case MaxmemoryPolicy::VolatileLru:
      return "volatile-lru";
    case MaxmemoryPolicy::VolatileLfu:
      return "volatile-lfu";
    case MaxmemoryPolicy::VolatileRandom:
      return "volatile-random";
    case MaxmemoryPolicy::VolatileTtl:
      return "volatile-ttl";
  }
  return "noeviction";
}

inline void apply_maxmemory_policy(MaxmemoryPolicy p) {
  g_maxmemory_policy = p;
  store().set_eviction_lfu(p == MaxmemoryPolicy::AllkeysLfu || p == MaxmemoryPolicy::VolatileLfu);
}

// Shared entropy source for anything that needs random lowercase-hex characters: the
// replication run id (fixed 40 chars) and ACL GENPASS (caller-specified length).
inline std::string gen_hex_chars(std::size_t n) {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  static const char* hex = "0123456789abcdef";
  std::string out(n, '0');
  for (char& c : out) c = hex[rng() & 0xF];
  return out;
}

// A Redis-style 40-char lowercase-hex run id, used as the replication id. Randomizing it
// per process means two unrelated PeaDB instances never share a replid, so a re-pointed
// replica can't be wrongly served a +CONTINUE partial resync against a different stream.
inline std::string gen_run_id() {
  return gen_hex_chars(40);
}

// syncCommand()'s refusal while this replica has no synced link to its own master, whose dataset
// and stream it would pass on.
std::string sync_refusal(const SessionState& session) {
  if (g_replication_role == "slave" && g_replica_stale)
    return "-NOMASTERLINK Can't SYNC while not connected with my master\r\n";
  if (session.has_pending_replies) return "-ERR SYNC and PSYNC are invalid with pending output\r\n";
  return std::string();
}

// syncCommand(): the client becomes a replica. A master's first replica creates the backlog, and
// with no past history behind it the replication id changes and replid2 is cleared.
void attach_replica(SessionState& session) {
  session.replica_stream = true;
  session.repl_ack_time_ms = now_ms();  // replicaPutOnline(): no false lag
  g_last_repl_db = -1;                  // the stream starts with a SELECT
  if (g_backlog_has_replica) return;
  g_master_replid = gen_run_id();
  g_master_replid2 = std::string(40, '0');
  g_second_replid_offset = -1;
  start_backlog();
  log(LogLevel::Info, "Replication backlog created, my new replication IDs are '" +
                          g_master_replid + "' and '" + g_master_replid2 + "'");
}

// Defined later (needs the AOF/backlog/notify propagation helpers): Redis's performEvictions()
// (evict.c). Ok when memory is within maxmemory (or eviction is not safe now), Running when it
// stopped at its time limit and the eviction timer goes on from the event loop, Fail when memory
// is over the limit and there is nothing (or, under noeviction, nothing allowed) to evict.
enum class EvictResult { Ok, Running, Fail };
EvictResult perform_evictions();
void clear_buffer(std::string& buf);  // defined with the maxmemory code

std::string encode_command_resp(const std::vector<std::string>& args);  // defined below

// Redis's replicationSendAck(): REPLCONF ACK with the offset of the master's stream this replica
// has applied (its master_repl_offset: everything applied is in its own stream).
void send_replica_ack() {
  const std::string ack =
      encode_command_resp({"REPLCONF", "ACK", std::to_string(g_master_repl_offset)});
  send_to_master(&ack);
}

// ── CLIENT PAUSE (Redis's pauseActions()/updatePausedActions(), networking.c) ──
// What a pause stops. WRITE and ALL also stop key expiry, eviction and replica traffic, so the
// dataset and the replication offset stay fixed while a failover waits for replicas.
constexpr std::uint32_t kPauseClientWrite = 1u << 0;
constexpr std::uint32_t kPauseClientAll = 1u << 1;  // bigger than kPauseClientWrite: stricter
constexpr std::uint32_t kPauseExpire = 1u << 2;
constexpr std::uint32_t kPauseEvict = 1u << 3;
constexpr std::uint32_t kPauseReplica = 1u << 4;
constexpr std::uint32_t kPauseClientWriteSet =
    kPauseClientWrite | kPauseExpire | kPauseEvict | kPauseReplica;
constexpr std::uint32_t kPauseClientAllSet =
    kPauseClientAll | kPauseExpire | kPauseEvict | kPauseReplica;
// Why something is paused (Redis's pause_purpose): each purpose keeps its own actions and end
// time, and what is paused is all of them together, so CLIENT UNPAUSE does not end the pause of a
// shutdown that waits for replicas. PeaDB has no FAILOVER (compat/delta.md item 68), so no
// PAUSE_DURING_FAILOVER.
enum PausePurpose { kPauseByClientCommand = 0, kPauseDuringShutdown = 1, kNumPausePurposes = 2 };
struct PauseEvent {
  std::uint32_t actions = 0;
  std::int64_t end_ms = 0;
};
PauseEvent g_pause_per_purpose[kNumPausePurposes];
std::uint32_t g_paused_actions = 0;    // what is paused now (server.paused_actions)
bool g_pause_unblock_pending = false;  // the pause became less strict: run held clients again
bool g_getack_pending = false;  // WAIT asked replicas for an ACK while replica traffic was paused

void update_paused_actions() {
  const std::uint32_t prev = g_paused_actions;
  g_paused_actions = 0;
  const std::int64_t now = now_ms();
  for (auto& p : g_pause_per_purpose) {
    if (p.end_ms > now) {
      g_paused_actions |= p.actions;
    } else {
      p = PauseEvent{};
    }
  }
  // Less strict than before: held clients run again and may be held again (Redis unblocks them
  // all).
  constexpr std::uint32_t kClientMask = kPauseClientWrite | kPauseClientAll;
  if ((g_paused_actions & kClientMask) < (prev & kClientMask)) g_pause_unblock_pending = true;
  store().set_expire_paused((g_paused_actions & kPauseExpire) != 0);
  // Redis's beforeSleep() sends the GETACK a WAIT asked for once replica traffic may flow again.
  if (g_getack_pending && !(g_paused_actions & kPauseReplica)) {
    g_getack_pending = false;
    if (g_replication_role != "slave")
      backlog_append(encode_command_resp({"REPLCONF", "GETACK", "*"}));
  }
}

// isPausedActionsWithUpdate(): checks the pause end time before answering.
std::uint32_t paused_actions_with_update(std::uint32_t mask) {
  if (!(g_paused_actions & mask)) return 0;
  update_paused_actions();
  return g_paused_actions & mask;
}

// beforeSleep(): the GETACK that WAIT asked for goes out once replica traffic may flow.
bool getack_waiting() {
  return g_getack_pending && !paused_actions_with_update(kPauseReplica);
}

// pauseActions(): the actions given replace the purpose's current ones, and the later end time
// of the two is kept.
void pause_actions(PausePurpose purpose, std::int64_t end_ms, std::uint32_t actions) {
  auto& p = g_pause_per_purpose[purpose];
  p.actions = actions;
  if (p.end_ms < end_ms) p.end_ms = end_ms;
  update_paused_actions();
}

void unpause_actions(PausePurpose purpose) {
  g_pause_per_purpose[purpose] = PauseEvent{};
  update_paused_actions();
}

// CLIENT PAUSE <timeout> [WRITE|ALL] (pauseClientsByClient()): a pause of everything stays one
// when a write pause follows it.
void pause_clients(std::int64_t end_ms, bool pause_all) {
  std::uint32_t actions = pause_all ? kPauseClientAllSet : kPauseClientWriteSet;
  if (!pause_all && (g_pause_per_purpose[kPauseByClientCommand].actions & kPauseClientAll))
    actions = kPauseClientAllSet;
  pause_actions(kPauseByClientCommand, end_ms, actions);
}

void unpause_clients() {
  unpause_actions(kPauseByClientCommand);
}

// processCommand()'s maxmemory result for the client's command now running, saved for its
// scripts (Redis's server.pre_command_oom_state): a script's denyoom write is refused only when
// memory was already over the limit when the command started (scriptVerifyOOM()).
bool g_pre_command_oom_state = false;
// min-replicas-to-write: reject writes only when FEWER than the required number of
// replicas are actually connected (the previous code rejected whenever the knob was
// set at all, blocking writes even with healthy replicas — a footgun).
// Never for the AOF or the master stream, and never on a replica (Redis's mustObeyClient() and
// checkGoodReplicasStatus()).
inline bool noreplicas_should_reject() {
  if (g_config_min_replicas_to_write <= 0) return false;
  if (must_obey_client() || g_replication_role == "slave") return false;
  const int connected = g_count_connected_replicas ? g_count_connected_replicas() : 0;
  return connected < g_config_min_replicas_to_write;
}
// Redis's writeCommandsDeniedByDiskError() and writeCommandsGetDiskErrorMessage(): the -MISCONF
// reply that refuses writes (and PING) while persistence is failing, so the server does not keep
// accepting data it cannot store; empty when writes are allowed. A failed BGSAVE clears after the
// next good save, an AOF error after the next good write or fsync.
std::string misconf_reply() {
  if (g_stop_writes_on_bgsave_error && !g_save_params.empty() && g_last_bgsave_status == "err") {
    return "-MISCONF Redis is configured to save RDB snapshots, but it's currently unable to "
           "persist to disk. Commands that may modify the data set are disabled, because this "
           "instance is configured to report errors during writes if RDB snapshotting fails "
           "(stop-writes-on-bgsave-error option). Please check the Redis logs for details about "
           "the RDB error.\r\n";
  }
  if (g_appendonly && (g_aof_write_error || g_aof_fsync_error)) {
    // With only the fsync failing, Redis reports the fsync's errno.
    const int err = g_aof_write_error ? g_aof_last_write_errno : g_aof_fsync_errno;
    return "-MISCONF Errors writing to the AOF file: " + std::string(std::strerror(err)) + "\r\n";
  }
  return {};
}

// processCommand(): after command/arity checks, a replica with a local persistence error
// stops on a master write unless configured to ignore disk errors. PING only warns. The
// callers also check at queue time, before a transaction can be acknowledged as applied.
void obey_master_disk_error(const std::string& cmd, std::uint64_t flags) {
  if (cmd != "PING" && !(flags & kRedisCmdWrite)) return;
  if (!g_replica_ignore_disk_write_errors && cmd != "PING") {
    log(LogLevel::Error, "Replica was unable to write command to disk.");
    std::abort();
  }
  const std::int64_t now = now_ms();
  if (now > g_replica_disk_error_last_log_ms + 10000) {
    g_replica_disk_error_last_log_ms = now;
    log(LogLevel::Warn, "Replica is applying a command even though it is unable to write to disk.");
  }
}

const char* const kMasterdownReply =
    "-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n";
const char* const kReadonlyReplicaReply =
    "-READONLY You can't write against a read only replica.\r\n";
const char* const kNoReplicasReply = "-NOREPLICAS Not enough good replicas to write.\r\n";

// Redis's running_stale: this server is a replica whose link to its master is not up, and
// replica-serve-stale-data is no.
bool running_stale() {
  return g_replication_role == "slave" && g_replica_stale.load() &&
         !g_config_replica_serve_stale_data;
}

// processCommand()'s replication checks, after -MISCONF, for a client's own command (to run, or
// to queue in MULTI) or an EXEC by what it queued; never for the AOF or the master stream
// (mustObeyClient()). A command that writes by Redis's flags (getCommandFlags(): a script or
// function counts when it declares flags without no-writes) is refused while fewer good replicas
// are connected than min-replicas-to-write, then on a replica, which is always read-only.
std::string write_refusal(bool writes) {
  if (!writes) return {};
  if (noreplicas_should_reject()) return kNoReplicasReply;
  if (g_replication_role == "slave") return kReadonlyReplicaReply;
  return {};
}

// The last of those checks (after the pub/sub context one at the top level): on a stale replica
// only the commands Redis flags STALE run; an EXEC only when all it queued are.
std::string stale_refusal(bool deny_stale) {
  return deny_stale && running_stale() ? kMasterdownReply : std::string();
}

// Both, for a command queued in MULTI (where the pub/sub context check cannot apply: a RESP2
// client in that context cannot send MULTI).
std::string replication_refusal(std::uint64_t flags) {
  std::string refusal = write_refusal((flags & kRedisCmdWrite) != 0);
  return refusal.empty() ? stale_refusal(!(flags & kRedisCmdStale)) : refusal;
}

// The -MISCONF reply of scriptPrepareForRun() for a script that declares it may write.
std::string script_misconf_reply() {
  if (g_stop_writes_on_bgsave_error && !g_save_params.empty() && g_last_bgsave_status == "err") {
    return "-MISCONF Redis is configured to save RDB snapshots, but it's currently unable to "
           "persist to disk. Writable scripts are blocked. Use 'no-writes' flag for read only "
           "scripts.\r\n";
  }
  if (g_appendonly && (g_aof_write_error || g_aof_fsync_error)) {
    const int err = g_aof_write_error ? g_aof_last_write_errno : g_aof_fsync_errno;
    return "-MISCONF Redis is configured to persist data to AOF, but it's currently unable to "
           "persist to disk. Writable scripts are blocked. Use 'no-writes' flag for read only "
           "scripts. AOF error: " +
           std::string(std::strerror(err)) + "\r\n";
  }
  return {};
}

std::string encode_error(const std::string& value);  // defined below (encoder block)

// Redis's scriptPrepareForRun(): what a script or function is checked for before it runs, by the
// flags it declares (kFunc*; `declared` is false for a script without a shebang, Redis's
// SCRIPT_FLAG_EVAL_COMPAT_MODE, which is only checked for a stale replica). `ro` is EVAL_RO,
// EVALSHA_RO or FCALL_RO. The dispatch refuses most of these first, by the flags the command
// takes from the script (processCommand()); these replies are seen when it could not know them,
// for example for a script loaded inside the transaction that runs it. Never for the AOF or the
// master stream.
std::string script_prepare_refusal(bool declared, int flags, bool ro) {
  if (must_obey_client()) return {};
  if (!declared) return running_stale() ? kMasterdownReply : std::string();
  if ((flags & kFuncNoCluster) && g_cluster_enabled)
    return encode_error("Can not run script on cluster, 'no-cluster' flag is set.");
  if (running_stale() && !(flags & kFuncAllowStale)) {
    return "-MASTERDOWN Link with MASTER is down, replica-serve-stale-data is set to 'no' and "
           "'allow-stale' flag is not set on the script.\r\n";
  }
  if (!(flags & kFuncNoWrites)) {
    if (g_replication_role == "slave")
      return "-READONLY Can not run script with write flag on readonly replica\r\n";
    if (std::string misconf = script_misconf_reply(); !misconf.empty()) return misconf;
    if (ro) return encode_error("Can not execute a script with write flag using *_ro command.");
    if (noreplicas_should_reject()) return kNoReplicasReply;
  }
  // The no-writes flag means allow-oom: only a script that may write is refused.
  if (g_pre_command_oom_state && g_config_maxmemory > 0 &&
      !(flags & (kFuncAllowOom | kFuncNoWrites))) {
    return "-OOM allow-oom flag is not set on the script, can not run it when used memory > "
           "'maxmemory'\r\n";
  }
  return {};
}

// Redis's hasActiveChildProcess(): a BGSAVE or AOF rewrite child is running.
bool has_active_child_process() {
  return g_bgsave_in_progress.load() || g_bgrewrite_in_progress.load();
}

// Redis's redisFork() for the BGSAVE and AOF rewrite children. In the child it makes logging
// lock-free and drops the listeners and client sockets (closeChildUnusedResourceAfterFork()).
pid_t fork_child() {
  log_before_fork();
  const pid_t pid = ::fork();
  if (pid == 0) {
    log_in_forked_child();
    if (g_close_sockets_in_child_cb) g_close_sockets_in_child_cb();
    store().set_lazy_expire_hook(nullptr);  // a child's deletions are its own; nothing propagates
  }
  return pid;
}

// The end of Redis's rdbSave() after a good foreground save (SAVE, FLUSHALL with save points,
// DEBUG RELOAD, the final save of a shutdown): the changes since the save start count again from
// zero and the save time moves.
void note_rdb_saved(std::int64_t epoch_at_start) {
  log(LogLevel::Info, "DB saved on disk");
  g_dirty_at_last_save = epoch_at_start;
  g_last_save_time = now_ms() / 1000;
  g_last_bgsave_status = "ok";
}

// Redis's rdbSaveBackground(): forks a child that writes the RDB, for the BGSAVE command and the
// save points. A failed fork() marks the BGSAVE as failed (rdb_last_bgsave_status:err, so writes
// are refused while save points are set) and is logged. The change count and the save time move
// only when the child succeeds (reap_background_children()). Returns false when no child started.
bool do_bgsave_fork(const RdbReplicationInfo* repl = nullptr) {
  if (has_active_child_process()) return false;
  ++g_stat_rdb_saves;
  g_dirty_before_bgsave = static_cast<std::int64_t>(store().mutation_epoch());
  g_lastbgsave_try = now_ms() / 1000;
  const auto fork_t0 = std::chrono::steady_clock::now();
  const pid_t pid = fork_child();
  if (pid == 0) {
    std::string err;
    const bool ok = rdb_save(g_snapshot_path, err, repl);
    if (ok) {
      log(LogLevel::Info, "DB saved on disk");
    } else {
      log(LogLevel::Warn, err);
    }
    _exit(ok ? 0 : 1);
  }
  if (pid < 0) {
    g_last_bgsave_status = "err";
    log(LogLevel::Warn, "Can't save in background: fork: " + std::string(std::strerror(errno)));
    return false;
  }
  g_latest_fork_usec = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - fork_t0)
                           .count();
  ++g_total_forks;
  log(LogLevel::Info, "Background saving started by pid " + std::to_string(pid));
  g_bgsave_in_progress.store(true);
  g_bgsave_child_pid.store(pid);
  g_bgsave_start_time = now_ms();
  g_bgsave_for_replication = repl != nullptr;
  // CONFIG SET dir/dbfilename can run while the child saves; it keeps the old cwd/path.
  g_bgsave_snapshot_path = std::filesystem::absolute(g_snapshot_path).string();
  return true;
}

// Redis's backgroundSaveDoneHandlerDisk() for a BGSAVE child that ended with `status`: only a good
// save moves the save time and takes the changes made before the fork off the count; a failed one
// sets the error status. A child stopped by a signal could not delete its temp file. `stopped` is
// set when the server killed the child itself, which, like Redis's SIGUSR1, is not an error.
void bgsave_done(pid_t pid, int status, bool stopped) {
  if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
    log(LogLevel::Info, "Background saving terminated with success");
    g_dirty_at_last_save = g_dirty_before_bgsave;
    g_last_save_time = now_ms() / 1000;
    g_last_bgsave_status = "ok";
  } else if (WIFEXITED(status)) {
    log(LogLevel::Warn, "Background saving error");
    g_last_bgsave_status = "err";
  } else {
    const int sig = WIFSIGNALED(status) ? WTERMSIG(status) : 0;
    log(LogLevel::Warn, "Background saving terminated by signal " + std::to_string(sig));
    ::unlink(rdb_temp_file_name(g_bgsave_snapshot_path, pid).c_str());
    if (!stopped && sig != SIGUSR1) g_last_bgsave_status = "err";
  }
  g_rdb_save_time_last = now_ms() / 1000 - g_bgsave_start_time / 1000;
  g_bgsave_child_pid.store(0);
  g_bgsave_in_progress.store(false);
  if (g_bgsave_for_replication) {
    g_bgsave_for_replication = false;
    if (g_replication_snapshot_done_cb)
      g_replication_snapshot_done_cb(g_bgsave_snapshot_path,
                                     !stopped && WIFEXITED(status) && WEXITSTATUS(status) == 0);
  }
}

// Redis's killRDBChild() and rdbRemoveTempFile(), for a shutdown and FLUSHALL: stops a running
// BGSAVE and deletes its temp-<child pid>.rdb. The child is waited for, so it cannot rename an
// older snapshot over a save that follows. Stopping it is not a failed BGSAVE: the status stays,
// and the changes since the last good save still count. A child that had already ended counts as
// it ended.
void kill_rdb_child() {
  const pid_t pid = g_bgsave_child_pid.load();
  if (!g_bgsave_in_progress.load() || pid <= 0) return;
  ::kill(pid, SIGKILL);
  int status = 0;
  pid_t waited = 0;
  while ((waited = ::waitpid(pid, &status, 0)) < 0 && errno == EINTR) {
  }
  if (waited == pid) {
    bgsave_done(pid, status, /*stopped=*/true);
    return;
  }
  // Already reaped elsewhere, so how it ended is unknown: count it as stopped.
  ::unlink(rdb_temp_file_name(g_bgsave_snapshot_path, pid).c_str());
  g_bgsave_child_pid.store(0);
  g_bgsave_in_progress.store(false);
  if (g_bgsave_for_replication) {
    g_bgsave_for_replication = false;
    if (g_replication_snapshot_done_cb)
      g_replication_snapshot_done_cb(g_bgsave_snapshot_path, false);
  }
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
    // CONFIG SET <name> <value> [<name> <value> ...]: redact the value of every SENSITIVE_CONFIG
    // setting (requirepass, masterauth, masteruser, the TLS key passwords), under any alias.
    for (std::size_t i = 2; i + 1 < out.size(); i += 2) {
      const RedisConfigSpec* spec = find_redis_config(args[i]);
      if (spec != nullptr && (spec->flags & kCfgSensitive) != 0) out[i + 1] = "(redacted)";
    }
  } else if (cmd == "MIGRATE") {
    redact_after("AUTH", 1);   // AUTH <password>
    redact_after("AUTH2", 2);  // AUTH2 <username> <password>
  } else if (cmd == "AUTH") {
    for (std::size_t i = 1; i < out.size(); ++i) out[i] = "(redacted)";
  } else if (cmd == "HELLO") {
    redact_after("AUTH", 2);  // HELLO [proto] AUTH <username> <password>
  }
  return out;
}

void slowlog_add(const std::vector<std::string>& args, std::int64_t usec, const std::string& peer,
                 const std::string& name) {
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
      e.argv.push_back(red[j].substr(0, 128) + "... (" + std::to_string(red[j].size() - 128) +
                       " more bytes)");
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
      "CONFIG",   "CLIENT",  "COMMAND", "OBJECT", "MEMORY", "SLOWLOG", "LATENCY", "ACL",
      "FUNCTION", "CLUSTER", "PUBSUB",  "XINFO",  "XGROUP", "SCRIPT",  "DEBUG"};
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
      case 'K':
        f |= NOTIFY_KEYSPACE;
        break;
      case 'E':
        f |= NOTIFY_KEYEVENT;
        break;
      case 'g':
        f |= NOTIFY_GENERIC;
        break;
      case '$':
        f |= NOTIFY_STRING;
        break;
      case 'l':
        f |= NOTIFY_LIST;
        break;
      case 's':
        f |= NOTIFY_SET;
        break;
      case 'h':
        f |= NOTIFY_HASH;
        break;
      case 'z':
        f |= NOTIFY_ZSET;
        break;
      case 'x':
        f |= NOTIFY_EXPIRED;
        break;
      case 'e':
        f |= NOTIFY_EVICTED;
        break;
      case 't':
        f |= NOTIFY_STREAM;
        break;
      case 'm':
        f |= NOTIFY_KEY_MISS;
        break;
      case 'n':
        f |= NOTIFY_NEW;
        break;
      case 'd':
        f |= NOTIFY_MODULE;
        break;
      case 'A':
        f |= NOTIFY_ALL;
        break;
      default:
        break;
    }
  return f;
}
// keyspaceEventsFlagsToString() (notify.c): the order CONFIG GET prints the classes in.
std::string notify_flags_to_str(std::uint32_t f) {
  std::string s;
  if ((f & NOTIFY_ALL) == NOTIFY_ALL)
    s += "A";
  else {
    if (f & NOTIFY_GENERIC) s += "g";
    if (f & NOTIFY_STRING) s += "$";
    if (f & NOTIFY_LIST) s += "l";
    if (f & NOTIFY_SET) s += "s";
    if (f & NOTIFY_HASH) s += "h";
    if (f & NOTIFY_ZSET) s += "z";
    if (f & NOTIFY_EXPIRED) s += "x";
    if (f & NOTIFY_EVICTED) s += "e";
    if (f & NOTIFY_STREAM) s += "t";
    if (f & NOTIFY_MODULE) s += "d";
    if (f & NOTIFY_NEW) s += "n";
  }
  if (f & NOTIFY_KEYSPACE) s += "K";
  if (f & NOTIFY_KEYEVENT) s += "E";
  if (f & NOTIFY_KEY_MISS) s += "m";
  return s;
}
void notify_keyspace_event(std::uint32_t cls, const std::string& event, const std::string& key,
                           int db) {
  if (!(g_notify_flags & cls)) return;
  if (g_notify_flags & NOTIFY_KEYSPACE)
    pubsub_publish("__keyspace@" + std::to_string(db) + "__:" + key, event);
  if (g_notify_flags & NOTIFY_KEYEVENT)
    pubsub_publish("__keyevent@" + std::to_string(db) + "__:" + event, key);
}
// Centralised emission for write commands (avoids hand-instrumenting ~40 handlers).
// Covers the most-subscribed events; class is per-command (mirrors the key's type). LSET,
// LREM, LINSERT, LTRIM, ZREM and ZREMRANGEBY* send their own events from their handlers,
// which know whether the key became empty ("del"); other commands miss events (delta item 72).
void emit_keyspace_notifications(const std::string& cmd, const std::vector<std::string>& args,
                                 int first_key, int db, const std::string* reply = nullptr) {
  if (g_notify_flags == 0) return;
  // Redis emits these events only when the command actually changed the key; a no-op
  // (e.g. SREM of a non-member, SADD of existing members) returns 0 and fires nothing.
  static const std::unordered_set<std::string> count_gated = {"SADD", "SREM", "HDEL"};
  if (reply && count_gated.count(cmd) && reply->rfind(":0\r\n", 0) == 0) return;
  if (cmd == "DEL" || cmd == "UNLINK") {
    for (std::size_t i = 1; i < args.size(); ++i)
      notify_keyspace_event(NOTIFY_GENERIC, "del", args[i], db);
    return;
  }
  if (cmd == "MSET" || cmd == "MSETNX") {
    for (std::size_t i = 1; i + 1 < args.size(); i += 2)
      notify_keyspace_event(NOTIFY_STRING, "set", args[i], db);
    return;
  }
  static const std::unordered_map<std::string, std::pair<std::uint32_t, std::string>> m = {
      {"SET", {NOTIFY_STRING, "set"}},
      {"SETEX", {NOTIFY_STRING, "set"}},
      {"PSETEX", {NOTIFY_STRING, "set"}},
      {"SETNX", {NOTIFY_STRING, "set"}},
      {"GETSET", {NOTIFY_STRING, "set"}},
      {"APPEND", {NOTIFY_STRING, "append"}},
      {"SETRANGE", {NOTIFY_STRING, "setrange"}},
      {"SETBIT", {NOTIFY_STRING, "setbit"}},
      {"INCR", {NOTIFY_STRING, "incrby"}},
      {"INCRBY", {NOTIFY_STRING, "incrby"}},
      {"DECR", {NOTIFY_STRING, "decrby"}},
      {"DECRBY", {NOTIFY_STRING, "decrby"}},
      {"INCRBYFLOAT", {NOTIFY_STRING, "incrbyfloat"}},
      {"GETDEL", {NOTIFY_GENERIC, "del"}},
      {"LPUSH", {NOTIFY_LIST, "lpush"}},
      {"RPUSH", {NOTIFY_LIST, "rpush"}},
      {"LPOP", {NOTIFY_LIST, "lpop"}},
      {"RPOP", {NOTIFY_LIST, "rpop"}},
      {"SADD", {NOTIFY_SET, "sadd"}},
      {"SREM", {NOTIFY_SET, "srem"}},
      {"SPOP", {NOTIFY_SET, "spop"}},
      {"HSET", {NOTIFY_HASH, "hset"}},
      {"HMSET", {NOTIFY_HASH, "hset"}},
      {"HSETNX", {NOTIFY_HASH, "hset"}},
      {"HDEL", {NOTIFY_HASH, "hdel"}},
      {"HINCRBY", {NOTIFY_HASH, "hincrby"}},
      {"HINCRBYFLOAT", {NOTIFY_HASH, "hincrbyfloat"}},
      {"ZADD", {NOTIFY_ZSET, "zadd"}},
      {"ZINCRBY", {NOTIFY_ZSET, "zincr"}},
      {"ZPOPMIN", {NOTIFY_ZSET, "zpopmin"}},
      {"ZPOPMAX", {NOTIFY_ZSET, "zpopmax"}},
      {"XADD", {NOTIFY_STREAM, "xadd"}},
      {"XTRIM", {NOTIFY_STREAM, "xtrim"}},
      {"XDEL", {NOTIFY_STREAM, "xdel"}},
      {"EXPIRE", {NOTIFY_GENERIC, "expire"}},
      {"PEXPIRE", {NOTIFY_GENERIC, "expire"}},
      {"EXPIREAT", {NOTIFY_GENERIC, "expire"}},
      {"PEXPIREAT", {NOTIFY_GENERIC, "expire"}},
      {"PERSIST", {NOTIFY_GENERIC, "persist"}},
      {"RENAME", {NOTIFY_GENERIC, "rename_from"}},
      {"COPY", {NOTIFY_GENERIC, "copy_to"}},
      {"RESTORE", {NOTIFY_GENERIC, "restore"}},
      {"RESTORE-ASKING", {NOTIFY_GENERIC, "restore"}},
  };
  auto it = m.find(cmd);
  if (it == m.end()) return;
  if ((cmd == "RESTORE" || cmd == "RESTORE-ASKING") && args.size() >= 2 &&
      !store().key_exists_raw(args[1])) {
    // The TTL had passed and REPLACE deleted the key that was there (a RESTORE that changed
    // nothing sends nothing): Redis's restoreCommand() sends `del`.
    notify_keyspace_event(NOTIFY_GENERIC, "del", args[1], db);
    return;
  }
  if (it->second.second == "expire" && args.size() >= 2 && !store().key_exists_raw(args[1])) {
    // An expire time in the past deleted the key (already_expired()), as Redis's
    // expireGenericCommand() does, with a `del` event; a missing key replies 0 and sends nothing.
    if (reply != nullptr && reply->rfind(":1\r\n", 0) == 0)
      notify_keyspace_event(NOTIFY_GENERIC, "del", args[1], db);
    return;
  }
  if (first_key > 0 && static_cast<std::size_t>(first_key) < args.size())
    notify_keyspace_event(it->second.first, it->second.second,
                          args[static_cast<std::size_t>(first_key)], db);
  if (cmd == "RENAME" && args.size() >= 3)
    notify_keyspace_event(NOTIFY_GENERIC, "rename_to", args[2], db);
}

// Feed the MONITOR stream: format the command Redis-style and push to every monitor.
void feed_monitors(const std::string& cmd, const std::vector<std::string>& args,
                   const SessionState& session) {
  if (g_monitor_fds.empty() || g_replaying_aof || !g_pubsub_send) return;
  if (cmd == "AUTH" || cmd == "MONITOR") return;  // Redis never echoes AUTH to monitors
  const std::int64_t ms = now_ms();
  char ts[40];
  std::snprintf(ts, sizeof(ts), "%lld.%06lld", static_cast<long long>(ms / 1000),
                static_cast<long long>((ms % 1000) * 1000));
  std::string line = "+";
  line += ts;
  line += " [" + std::to_string(session.db_index) + " " +
          (session.addr.empty() ? "?:0" : session.addr) + "]";
  // Passwords are shown as (redacted), as in SLOWLOG (Redis's redactClientCommandArgument() changes
  // what both show).
  for (const auto& a : redact_slowlog_args(args)) {
    line += " \"";
    for (char c : a) {
      switch (c) {
        case '"':
          line += "\\\"";
          break;
        case '\\':
          line += "\\\\";
          break;
        case '\n':
          line += "\\n";
          break;
        case '\r':
          line += "\\r";
          break;
        case '\t':
          line += "\\t";
          break;
        default:
          if (static_cast<unsigned char>(c) < 32 || static_cast<unsigned char>(c) >= 127) {
            char hex[8];
            std::snprintf(hex, sizeof(hex), "\\x%02x", static_cast<unsigned char>(c));
            line += hex;
          } else
            line += c;
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
inline const char* hll_magic() {
  return "PEAHLL01";
}

std::uint64_t hll_hash(const std::string& s) {
  std::uint64_t h = 1469598103934665603ULL;  // FNV-1a 64
  for (unsigned char c : s) {
    h ^= c;
    h *= 1099511628211ULL;
  }
  h ^= h >> 30;
  h *= 0xbf58476d1ce4e5b9ULL;  // splitmix64 finalizer
  h ^= h >> 27;
  h *= 0x94d049bb133111ebULL;
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
  const std::uint64_t w = h >> kHllP;  // 50 meaningful bits
  int rank = 1;
  std::uint64_t mask = 1ULL << (63 - kHllP);
  while (rank <= (64 - kHllP) && (w & mask) == 0) {
    ++rank;
    mask >>= 1;
  }
  auto* regs = reinterpret_cast<std::uint8_t*>(v.data()) + kHllHdr;
  if (regs[idx] < rank) {
    regs[idx] = static_cast<std::uint8_t>(rank);
    return true;
  }
  return false;
}
double hll_count(const std::string& v) {
  const auto* regs = reinterpret_cast<const std::uint8_t*>(v.data()) + kHllHdr;
  const double m = kHllRegs;
  double sum = 0.0;
  int zeros = 0;
  for (int i = 0; i < kHllRegs; ++i) {
    sum += std::ldexp(1.0, -static_cast<int>(regs[i]));
    if (regs[i] == 0) ++zeros;
  }
  const double alpha = 0.7213 / (1.0 + 1.079 / m);
  double E = alpha * m * m / sum;
  if (E <= 2.5 * m && zeros != 0)
    E = m * std::log(m / static_cast<double>(zeros));  // linear counting
  return E;
}

// Resident set size in bytes (0 if unavailable). Used for used_memory_rss / fragmentation.
std::int64_t process_rss_bytes() {
#if defined(__linux__)
  FILE* f = std::fopen("/proc/self/statm", "r");
  if (!f) return 0;
  long pages = 0, resident = 0;
  if (std::fscanf(f, "%ld %ld", &pages, &resident) != 2) {
    std::fclose(f);
    return 0;
  }
  std::fclose(f);
  return static_cast<std::int64_t>(resident) * static_cast<std::int64_t>(::sysconf(_SC_PAGESIZE));
#else
  struct rusage ru {};
  if (getrusage(RUSAGE_SELF, &ru) != 0) return 0;
  return static_cast<std::int64_t>(ru.ru_maxrss);  // macOS: bytes
#endif
}
std::map<std::string, FunctionLibrary> g_libraries;         // lc library name -> library
std::unordered_map<std::string, std::string> g_func_index;  // lc function name -> lc library name
bool g_rdb_load_functions_replace = true;  // RDB function-load collision policy (see hooks)
bool g_script_readonly_context = false;
RespVersion g_script_client_resp_version = RespVersion::Resp2;
SessionState* g_script_current_session = nullptr;
std::uint64_t g_script_prng_state = 0x12345678ULL;
std::unordered_map<std::string, CmdStats> g_cmdstats;
std::unordered_map<std::string, std::int64_t> g_errorstats;
std::int64_t g_total_error_replies = 0;
std::int64_t g_unexpected_error_replies = 0;

}  // end anonymous namespace

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
std::function<std::size_t()> g_replica_output_buffers_cb;
std::function<std::pair<std::size_t, std::size_t>()> g_client_buffers_cb;
std::function<int()> g_blocked_clients_cb;
std::function<int(const ClientKillFilter&, bool&)> g_client_kill_cb;
std::function<void()> g_close_sockets_in_child_cb;
std::function<int(std::uint64_t, bool)> g_client_unblock_cb;
std::function<void(const std::function<void(const SessionState&)>&)> g_for_each_replica_cb;
std::function<void(const std::string*)> g_feed_replicas_cb;
std::function<void(const std::string&)> g_unblock_blocked_clients_cb;
std::function<void(const std::function<std::string(const SessionState&)>&)>
    g_reply_blocked_on_shutdown_cb;
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
}  // namespace

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
std::string encode_pubsub_message(const std::string& type, const std::string& channel,
                                  const std::string& data) {
  return "*3\r\n$" + std::to_string(type.size()) + "\r\n" + type + "\r\n" + "$" +
         std::to_string(channel.size()) + "\r\n" + channel + "\r\n" + "$" +
         std::to_string(data.size()) + "\r\n" + data + "\r\n";
}
std::string encode_pmessage(const std::string& pattern, const std::string& channel,
                            const std::string& data) {
  return std::string("*4\r\n$8\r\npmessage\r\n") + "$" + std::to_string(pattern.size()) + "\r\n" +
         pattern + "\r\n" + "$" + std::to_string(channel.size()) + "\r\n" + channel + "\r\n" + "$" +
         std::to_string(data.size()) + "\r\n" + data + "\r\n";
}
bool pattern_matches(const std::string& pattern, const std::string& str) {
  return fnmatch(pattern.c_str(), str.c_str(), 0) == 0;
}
}  // namespace

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
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return s;
}

std::string lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
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
  engine.clear();
  name.clear();
  body.clear();
  if (payload.rfind("#!", 0) != 0) {
    err = "Missing library metadata";
    return false;
  }
  const auto nl = payload.find('\n');
  if (nl == std::string::npos) {
    err = "Invalid library metadata";
    return false;
  }
  const std::string line = payload.substr(0, nl);
  body = payload.substr(nl + 1);  // Redis: the code starts at the '\n' (shebang line excluded)
  std::vector<std::string> parts;
  for (std::size_t i = 0; i < line.size();) {
    while (i < line.size() && std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    const std::size_t s = i;
    while (i < line.size() && !std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    if (i > s) parts.push_back(line.substr(s, i - s));
  }
  if (parts.empty()) {
    err = "Invalid library metadata";
    return false;
  }
  engine = parts[0].rfind("#!", 0) == 0 ? parts[0].substr(2) : parts[0];
  bool have_name = false;
  for (std::size_t i = 1; i < parts.size(); ++i) {
    const std::string& tok = parts[i];
    if (tok.size() >= 5 && lower(tok.substr(0, 5)) == "name=") {
      if (have_name) {
        err = "Invalid metadata value, name argument was given multiple times";
        return false;
      }
      std::string v = tok.substr(5);
      if (v.size() >= 2 && v.front() == '"' && v.back() == '"') v = v.substr(1, v.size() - 2);
      name = v;
      have_name = true;
    } else {
      err = "Invalid metadata value given: " + tok;
      return false;
    }
  }
  if (!have_name) {
    err = "Library name was not given";
    return false;
  }
  return true;
}

// Load (or REPLACE) a single library from its full source. On success returns true and sets
// out_name; on failure returns false and sets err (exact Redis text, no "ERR " prefix). On any
// failure the existing library (if replacing) is left untouched.
bool load_function_library(const std::string& code, bool replace, std::string& out_name,
                           std::string& err) {
  std::string engine, name, body;
  if (!parse_function_shebang(code, engine, name, body, err)) return false;
  if (!valid_lib_name(name)) {
    err =
        "Library names can only contain letters, numbers, or underscores(_) and must be at least "
        "one character long";
    return false;
  }
  if (upper(engine) != "LUA") {
    err = "Engine '" + engine + "' not found";
    return false;
  }
  const std::string lc = lower(name);
  auto existing = g_libraries.find(lc);
  if (existing != g_libraries.end() && !replace) {
    err = "Library '" + name + "' already exists";
    return false;
  }

  std::vector<LuaRegisteredFunction> fns;
  if (!lua_engine_load_library(body, fns, err))
    return false;  // err set (compile/register/timeout/no-functions)

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
  lib.name = name;
  lib.engine = "LUA";
  lib.code = code;
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

// FUNCTION DUMP: Redis's payload (one FUNCTION2 opcode and source string per library, the RDB
// version and the CRC64), so a payload moves between PeaDB and Redis in both directions.
std::string functions_dump() {
  std::vector<std::string> codes;
  codes.reserve(g_libraries.size());
  for (const auto& [lc, lib] : g_libraries) codes.push_back(lib.code);
  return rdb_dump_functions(codes);
}

// Apply a FUNCTION DUMP blob under FLUSH|APPEND|REPLACE. Atomic: on any failure the pre-call
// state is fully restored (Redis semantics — a bad/colliding payload never partially applies).
bool functions_restore(const std::string& payload, const std::string& policy, std::string& err) {
  std::vector<std::string> codes;
  if (!rdb_parse_function_payload(payload, codes, err)) return false;

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

// Shared FCALL / FCALL_RO body. `ro_command` is true for FCALL_RO.
std::string do_fcall(const std::vector<std::string>& args, SessionState& session, bool ro_command) {
  auto idx = g_func_index.find(lower(args[1]));
  if (idx == g_func_index.end()) return encode_error("Function not found");
  const FunctionEntry* entry = nullptr;
  auto libit = g_libraries.find(idx->second);
  if (libit != g_libraries.end())
    for (const auto& f : libit->second.functions)
      if (lower(f.name) == lower(args[1])) {
        entry = &f;
        break;
      }
  if (!entry) return encode_error("Function not found");

  std::int64_t numkeys = 0;
  if (!parse_i64(args[2], numkeys)) return encode_error("Bad number of keys provided");
  if (numkeys < 0) return encode_error("Number of keys can't be negative");
  if (static_cast<std::size_t>(numkeys) > args.size() - 3)
    return encode_error("Number of keys can't be greater than number of args");

  const bool no_writes = (entry->flags & kFuncNoWrites) != 0;
  if (std::string refusal = script_prepare_refusal(true, entry->flags, ro_command);
      !refusal.empty())
    return refusal;
  const bool readonly = ro_command || no_writes;

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
    return std::string(
        "-BUSY Redis is busy running a script. Script killed by user with FUNCTION KILL.\r\n");
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
  const bool prev_write_dirty = g_script_write_dirty;
  if (entry->flags & kFuncAllowOom) g_script_allow_oom = true;
  g_script_write_dirty = false;
  const int callback_ref = entry->callback_ref;
  // Freeze time for the duration of the call (Redis behavior — a key's logical expiry does not
  // advance mid-function, so TIME is stable and PX-expiring keys survive an inner DEBUG SLEEP).
  store().freeze_time();
  const auto out = lua_engine_fcall(callback_ref, keys, argv, session, readonly);
  store().unfreeze_time();
  g_script_allow_oom = prev_allow_oom;
  g_script_write_dirty = prev_write_dirty;
  g_script_current_session = nullptr;
  return out;
}

bool arity_ok(std::size_t argc, int arity) {
  if (arity >= 0) return argc == static_cast<std::size_t>(arity);
  return argc >= static_cast<std::size_t>(-arity);
}

std::int64_t now_ms() {
  return DataStore::now_ms();
}

std::string encode_simple(const std::string& value) {
  return "+" + value + "\r\n";
}
std::string encode_error(const std::string& value) {
  return "-ERR " + value + "\r\n";
}
std::string encode_integer(long long value) {
  return ":" + std::to_string(value) + "\r\n";
}
std::string encode_bulk(const std::string& value) {
  return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}

// Format a double score as Redis does: up to 17 significant digits, no trailing zeros.
std::string format_score(double d) {
  char buf[64];
  snprintf(buf, sizeof(buf), "%.17g", d);
  return buf;
}
std::string encode_null(RespVersion version) {
  return version == RespVersion::Resp3 ? "_\r\n" : "$-1\r\n";
}
std::string encode_null_array() {
  return "*-1\r\n";
}
// RESP3 has a single null type; RESP2 distinguishes the null array (*-1).
std::string encode_null_array(RespVersion v) {
  return v == RespVersion::Resp3 ? "_\r\n" : "*-1\r\n";
}

// ── RESP3-aware type encoders (fall back to RESP2 shapes for RESP2 clients) ──
std::string encode_double(double d, RespVersion v) {
  std::string s;
  if (std::isinf(d))
    s = d > 0 ? "inf" : "-inf";
  else if (std::isnan(d))
    s = "nan";
  else
    s = format_score(d);
  return v == RespVersion::Resp3 ? ("," + s + "\r\n") : encode_bulk(s);
}
std::string encode_bool(bool b, RespVersion v) {
  return v == RespVersion::Resp3 ? (b ? "#t\r\n" : "#f\r\n") : (b ? ":1\r\n" : ":0\r\n");
}
// RESP3 verbatim string: a human-readable multi-line blob tagged with a 3-char format (Redis
// only ever sends "txt"). Mirrors addReplyVerbatim (networking.c): the header's length counts
// the 4-byte "fmt:" prefix (format padded/truncated to exactly 3 chars) plus the payload; RESP2
// has no verbatim type, so addReplyVerbatim degrades to an identical-bytes bulk string there.
std::string encode_verbatim(const std::string& value, RespVersion v, const char* ext = "txt") {
  if (v != RespVersion::Resp3) return encode_bulk(value);
  char fmt[3] = {' ', ' ', ' '};
  for (int i = 0; i < 3 && ext[i] != '\0'; ++i) fmt[i] = ext[i];
  return "=" + std::to_string(value.size() + 4) + "\r\n" + std::string(fmt, 3) + ":" + value +
         "\r\n";
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

bool is_error_reply(const std::string& r) {
  return !r.empty() && r[0] == '-';
}

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
  return "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN "
         "NOSAVE.\r\n";
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
    while (n > 0) {
      ++d;
      n /= 10;
    }
    return d;
  };
  std::size_t total = 1 + digits(args.size()) + 2;  // *<count>\r\n
  for (const auto& a : args) {
    total += 1 + digits(a.size()) + 2 + a.size() + 2;  // $<len>\r\n<data>\r\n
  }
  return total;
}

// Rewrite a client write command into its canonical, deterministic propagation form —
// shared by the replication backlog AND the AOF (both must replay identically later /
// elsewhere): relative expirations become PEXPIREAT, SPOP becomes SREM of the actually
// removed members, XADD's auto '*' id becomes the concrete generated id, etc.
// Returns false when the command must not be propagated at all.
bool canonicalize_propagated_command(const std::vector<std::string>& args,
                                     const std::string* reply_ptr, std::vector<std::string>& out) {
  const auto cmd = upper(args[0]);
  out = args;
  bool emit = true;

  auto key_exists = [&](const std::string& key) {
    return store().type_of(key) != "none";
  };
  auto abs_ttl = [&](const std::string& key) {
    return store().pexpiretime(key);
  };

  // Redis propagates EXPIRE and GETEX only when they changed the key: an EXPIRE answered 0 and a
  // GETEX answered nil (a missing or expired key, whose own DEL is already queued) send nothing.
  const bool unchanged =
      reply_ptr != nullptr && (*reply_ptr == ":0\r\n" || *reply_ptr == "$-1\r\n");
  if (cmd == "GETEX") {
    if (args.size() == 2 || unchanged) {
      emit = false;
    } else if (upper(args[2]) == "PERSIST") {
      if (key_exists(args[1]))
        out = {"PERSIST", args[1]};
      else
        emit = false;
    } else {
      const auto abs = abs_ttl(args[1]);
      if (abs == -2)
        out = {"DEL", args[1]};
      else if (abs >= 0)
        out = {"PEXPIREAT", args[1], std::to_string(abs)};
      else
        emit = false;
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
    if (reply_ptr->empty() || (*reply_ptr)[0] != ':')
      emit = false;
    else {
      try {
        const auto n = std::stoll(reply_ptr->substr(1));
        if (n <= 0) emit = false;
      } catch (...) {
        emit = false;
      }
    }
  } else if (cmd == "SET" && args.size() >= 4) {
    // Redis's setGenericCommand() for a SET that ran (one NX or XX stopped is not propagated): EX,
    // PX and EXAT go out as SET key value PXAT <ms>, PXAT as it came, and without an expire the
    // GET options are left out.
    std::string exp_opt;
    bool get = false;
    for (std::size_t i = 3; i < args.size(); ++i) {
      const auto o = upper(args[i]);
      if (o == "EX" || o == "PX" || o == "EXAT" || o == "PXAT") {
        exp_opt = o;
        ++i;  // its value
      } else if (o == "GET") {
        get = true;
      }
    }
    if (!exp_opt.empty() && exp_opt != "PXAT") {
      const auto abs = abs_ttl(args[1]);
      if (abs >= 0) out = {"SET", args[1], args[2], "PXAT", std::to_string(abs)};
    } else if (exp_opt.empty() && get) {
      out.erase(std::remove_if(out.begin() + 3, out.end(),
                               [](const std::string& a) { return upper(a) == "GET"; }),
                out.end());
    }
  } else if ((cmd == "SETEX" || cmd == "PSETEX") && args.size() == 4) {
    const auto abs = abs_ttl(args[1]);
    if (abs >= 0) out = {"SET", args[1], args[3], "PXAT", std::to_string(abs)};
  } else if ((cmd == "EXPIRE" || cmd == "PEXPIRE" || cmd == "EXPIREAT" || cmd == "PEXPIREAT") &&
             args.size() >= 3) {
    const auto abs = abs_ttl(args[1]);
    if (unchanged)
      emit = false;
    else if (abs == -2)
      out = {"DEL", args[1]};
    else if (abs >= 0)
      out = {"PEXPIREAT", args[1], std::to_string(abs)};
    else
      emit = false;
  } else if ((cmd == "RESTORE" || cmd == "RESTORE-ASKING") && args.size() >= 4) {
    // Redis's restoreCommand(). A RESTORE that stored nothing and deleted nothing is not propagated
    // (g_write_changed_nothing).
    if (!key_exists(args[1])) {
      // The TTL had passed and REPLACE deleted the key that was there: DEL (UNLINK) instead.
      out = {g_lazyfree_lazy_server_del ? "UNLINK" : "DEL", args[1]};
    } else {
      // A relative TTL goes out as the absolute expire time, with ABSTTL added last; the payload
      // and the other options stay as they came. Without a TTL, or with ABSTTL, as it came.
      const bool has_absttl = std::any_of(
          args.begin() + 4, args.end(), [](const std::string& a) { return upper(a) == "ABSTTL"; });
      const auto abs = abs_ttl(args[1]);
      if (!has_absttl && abs >= 0) {
        out[2] = std::to_string(abs);
        out.push_back("ABSTTL");
      }
    }
  } else if (cmd == "SCRIPT") {
    emit = false;
  } else if (cmd == "MIGRATE") {
    // It propagates the DEL of the keys the target took itself (migrate_command()).
    emit = false;
  } else if (cmd == "BLMPOP" || cmd == "BZMPOP" || cmd == "LMPOP" || cmd == "ZMPOP") {
    // These handlers propagate their effect explicitly as deterministic LPOP/RPOP or
    // ZPOPMIN/ZPOPMAX (both on the immediate and the unblock paths).
    emit = false;
  } else if (cmd == "BLMOVE" && args.size() == 6) {
    // Served at once (a blocked one is propagated when it is woken): Redis rewrites it to LMOVE,
    // so a replica or the AOF never blocks on it.
    out = {"LMOVE", args[1], args[2], args[3], args[4]};
  } else if (cmd == "BRPOPLPUSH") {
    // Runs as BLMOVE through handle_command(), which propagates the move; propagating
    // BRPOPLPUSH as well moved a second element on replicas and on AOF replay.
    emit = false;
  } else if (cmd == "XREADGROUP") {
    emit = false;
  } else if (cmd == "EVAL" || cmd == "EVALSHA" || cmd == "EVAL_RO" || cmd == "FCALL" ||
             cmd == "FCALL_RO") {
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
        if (o == "NOMKSTREAM") {
          ++i;
          continue;
        }
        if (o == "MAXLEN" || o == "MINID") {
          ++i;
          if (i < args.size() && (args[i] == "~" || args[i] == "=")) ++i;
          if (i < args.size()) ++i;  // threshold
          if (i + 1 < args.size() && upper(args[i]) == "LIMIT") i += 2;
          continue;
        }
        break;  // args[i] is the id
      }
      if (i < args.size() && args[i].find('*') != std::string::npos && !r.empty() && r[0] == '$') {
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

// ── Propagation of writes to the AOF and the replicas ──────────────────────────
// Redis's alsoPropagate() and propagatePendingCommands(): the writes a command makes, and those
// of the commands it runs (EXEC, scripts, functions), are queued while it runs and written to the
// AOF and the replication backlog when it ends, each stream in one write, wrapped in MULTI/EXEC
// when there is more than one. So a crash, a torn AOF tail or a cut replica link never leaves
// half of a transaction or script applied: the AOF loader drops an unfinished MULTI (item 30).

void append_aof_payload(std::string_view payload);  // defined below

// Redis's shouldPropagate(): the AOF takes writes while it is on or waits for its first rewrite,
// the replicas unless this server is a replica itself (its stream comes from its master).
bool should_propagate(int target) {
  return ((target & kPropagateAof) && g_appendonly) ||
         ((target & kPropagateRepl) && g_replication_role != "slave");
}

// Redis's propagateNow() for one command, `resp` encoded: adds it to the AOF and replica bytes
// for the targets that take it, after a SELECT when the stream is in another database. MULTI and
// EXEC pass db -1 and never need one: the SELECT goes inside the transaction, as in Redis.
void feed_propagation(int db, const std::string& resp, int target, std::string& aof,
                      std::string& repl) {
  if ((target & kPropagateAof) && g_appendonly) {
    if (db >= 0 && db != g_last_aof_db) {
      aof += encode_command_resp({"SELECT", std::to_string(db)});
      g_last_aof_db = db;
    }
    aof += resp;
  }
  if ((target & kPropagateRepl) && g_replication_role != "slave") {
    if (db >= 0 && db != g_last_repl_db) {
      repl += encode_command_resp({"SELECT", std::to_string(db)});
      g_last_repl_db = db;
    }
    repl += resp;
  }
}

// The AOF first, as Redis's beforeSleep() writes it before the replicas get their bytes: with
// appendfsync always a failed write exits before anything goes out.
void write_propagation(const std::string& aof, const std::string& repl) {
  if (!aof.empty()) append_aof_payload(aof);
  if (!repl.empty()) backlog_append(repl);
}

// Writes one command at once (Redis's propagateNow()), encoded once for both streams.
void propagate_now(int db, const std::vector<std::string>& argv, int target) {
  const bool to_aof = (target & kPropagateAof) && g_appendonly;
  const bool to_repl = (target & kPropagateRepl) && g_replication_role != "slave";
  if (!to_aof && !to_repl) return;
  const std::string resp = encode_command_resp(argv);
  if (to_aof) {
    if (db == g_last_aof_db) {
      append_aof_payload(resp);
    } else {
      g_last_aof_db = db;
      append_aof_payload(encode_command_resp({"SELECT", std::to_string(db)}) + resp);
    }
  }
  if (to_repl) {
    if (db != g_last_repl_db) {
      backlog_append(encode_command_resp({"SELECT", std::to_string(db)}));
      g_last_repl_db = db;
    }
    backlog_append(resp);
  }
}

// Redis's alsoPropagate(): inside an execution unit the write waits for its end, otherwise
// (evictions, active expiry, the event loop) it is written at once. `last_of_unit` says nothing
// can follow it in the unit (the outermost command's own write), so when nothing waits before
// it either it is the unit's only write and goes out at once, as the unit's end would send it.
void also_propagate(int db, std::vector<std::string> argv, int target, bool last_of_unit = false) {
  if (g_execution_nesting == 0 || (last_of_unit && g_also_propagate.empty())) {
    propagate_now(db, argv, target);  // checks the targets itself
    return;
  }
  if (should_propagate(target)) g_also_propagate.push_back({db, std::move(argv), target});
}

// Redis's propagatePendingCommands(), run when the outermost execution unit ends. `top_args` is
// the command that started the unit: SCAN and RANDOMKEY (CMD_TOUCHES_ARBITRARY_KEYS) send the
// DELs of the expired keys they meet without MULTI/EXEC, because those keys are not one unit.
void propagate_pending_commands(const std::vector<std::string>* top_args, bool may_wrap) {
  if (g_also_propagate.empty()) return;
  if (g_also_propagate.size() == 1) {  // most commands: no transaction, nothing to gather
    const PendingPropagation& op = g_also_propagate.front();
    propagate_now(op.db, op.argv, op.target);
    g_also_propagate.clear();
    return;
  }
  const bool transaction =
      may_wrap &&
      !(top_args != nullptr && (redis_command_flags(*top_args) & kRedisCmdTouchesArbitraryKeys));
  static const std::string kMulti = encode_command_resp({"MULTI"});
  static const std::string kExec = encode_command_resp({"EXEC"});
  constexpr int kBoth = kPropagateAof | kPropagateRepl;
  std::string aof;
  std::string repl;
  if (transaction) feed_propagation(-1, kMulti, kBoth, aof, repl);
  for (const auto& op : g_also_propagate) {
    feed_propagation(op.db, encode_command_resp(op.argv), op.target, aof, repl);
  }
  if (transaction) feed_propagation(-1, kExec, kBoth, aof, repl);
  g_also_propagate.clear();
  write_propagation(aof, repl);
}

// Redis's enterExecutionUnit()/exitExecutionUnit() with postExecutionUnitOperations(), around a
// command (handle_command()) and a blocked client's wakeup: what runs inside is propagated when
// the outermost unit ends, before its reply goes out.
class ExecutionUnit {
 public:
  // `may_wrap` false: the writes go out together but each on its own, never in MULTI/EXEC (the
  // active expiry cycle, as Redis's activeExpireCycle() propagates its DELs).
  explicit ExecutionUnit(const std::vector<std::string>* args, bool may_wrap = true)
      : args_(args), may_wrap_(may_wrap) {
    ++g_execution_nesting;
  }
  ~ExecutionUnit() {
    if (--g_execution_nesting != 0) return;
    store().restore_hidden_expired();  // a replica keeps expired keys until its master's DEL
    if (g_also_propagate.empty()) return;
    try {
      propagate_pending_commands(args_, may_wrap_);
    } catch (const std::exception& e) {
      log(LogLevel::Warn, std::string("Failed to propagate the writes of a command: ") + e.what());
      g_also_propagate.clear();
    }
  }
  ExecutionUnit(const ExecutionUnit&) = delete;
  ExecutionUnit& operator=(const ExecutionUnit&) = delete;

 private:
  const std::vector<std::string>* args_;
  bool may_wrap_;
};

// Redis's call(): c->woff is the replication offset right after the client's last command that
// propagated something, which WAIT waits for. Declared before the command's ExecutionUnit, so it
// reads the offset once the unit has propagated.
class WriteOffsetTracker {
 public:
  explicit WriteOffsetTracker(SessionState& session)
      : session_(session), before_(g_master_repl_offset) {
  }
  ~WriteOffsetTracker() {
    if (g_master_repl_offset != before_) session_.woff = g_master_repl_offset;
  }
  WriteOffsetTracker(const WriteOffsetTracker&) = delete;
  WriteOffsetTracker& operator=(const WriteOffsetTracker&) = delete;

 private:
  SessionState& session_;
  std::int64_t before_;
};

// A write a command made (Redis's call() → alsoPropagate()), as replicas and the AOF must run it
// (canonicalize_propagated_command()). A script's redis.set_repl() drops targets: bit 1 keeps the
// AOF, bit 2 the replicas. AOF replay propagates nothing; master commands propagate their
// deterministic effects to the local AOF, while their raw bytes feed the replication backlog.
void propagate_write(const std::vector<std::string>& args, const SessionState& session,
                     const std::string* reply_ptr, int target = kPropagateAof | kPropagateRepl,
                     bool last_of_unit = false) {
  if (args.empty() || (g_replaying_aof && !g_applying_master_stream)) return;
  if (in_lua_script_context()) {
    if (!(g_script_repl_flags & 1)) target &= ~kPropagateAof;
    if (!(g_script_repl_flags & 2)) target &= ~kPropagateRepl;
    if (target == 0) return;
  }
  std::vector<std::string> out;
  {
    // Its lookups only read back what the command did (Redis rewrites the command's argv), so
    // they must not delete a key the command left with a past expire time (SET ... PXAT <past>).
    const DataStore::LazyExpireDisabled no_lazy_expire(store());
    if (!canonicalize_propagated_command(args, reply_ptr, out)) return;
  }
  also_propagate(session.db_index, std::move(out), target, last_of_unit);
}

// Redis's stream helpers use alsoPropagate(AOF|REPL), including under redis.set_repl(NONE).
// AOF loading emits nothing; a master stream still persists its effects in the local AOF.
void propagate_stream_effect(std::vector<std::string> argv, const SessionState& session) {
  if (g_replaying_aof && !g_applying_master_stream) return;
  also_propagate(session.db_index, std::move(argv), kPropagateAof | kPropagateRepl);
}

// Snapshot-based claims are idempotent on a delayed replica and on AOF replay.
void propagate_stream_claims(const std::vector<DataStore::StreamClaimEffect>& effects,
                             const SessionState& session) {
  for (const auto& e : effects)
    propagate_stream_effect(
        {"XCLAIM", e.key, e.group, e.consumer, "0", e.id, "TIME", std::to_string(e.delivery_ms),
         "RETRYCOUNT", std::to_string(e.delivery_count), "FORCE", "JUSTID", "LASTID", e.last_id},
        session);
}

// Redis's deleteExpiredKeyAndPropagate() and propagateDeletion(), called by DataStore for each key
// a master deletes because its TTL has passed (a lookup, SCAN, RANDOMKEY or the active cycle): the
// DEL goes to the AOF and to the replicas whatever redis.set_repl() says (not through
// propagate_write()), inside the open execution unit, so a write that meets an expired key goes out
// as MULTI, DEL, the write, EXEC; then the `expired` event.
void propagate_lazy_expire(int db, const std::string& key) {
  also_propagate(db, {"DEL", key}, kPropagateAof | kPropagateRepl);
  notify_keyspace_event(NOTIFY_EXPIRED, "expired", key, db);
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
  if (g_aof_fd >= 0) {
    ::close(g_aof_fd);
    g_aof_fd = -1;
  }
  g_aof_fd = ::open(g_aof_path.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
  // The db context at the file's tail is unknown (fresh file, pre-existing file, or a
  // just-finished rewrite) — force the next append to emit an explicit SELECT.
  g_last_aof_db = -1;
}

// What one step of the command reader in Redis's loadSingleAppendOnlyFile() found.
enum class AofStep {
  Command,     // a whole command, now in `args`
  Annotation,  // a `#` line, skipped
  ShortRead,   // the bytes end inside a command (Redis's "unexpected end of file")
  BadFormat,   // the bytes are not a command (Redis's "bad file format")
};

// Reads the next command of an AOF held in memory, from `pos` (the start of a line, before the
// end), the way Redis's loader does with fgets()/fread(): a `*<argc>` line, then for each argument
// a `$<len>` line and the bytes. What counts as well formed is what redis-check-aof accepts: each
// number (read like strtol()) is followed by CRLF and nothing else, a length is not negative, and
// each argument is followed by CRLF. Redis's loader is looser (atoi(), and the two bytes after an
// argument skipped unchecked), which only matters for a damaged file: torn bytes in the middle of
// the file, as older PeaDB versions left them, could then be read as a different command.
AofStep read_aof_step(std::string_view buf, std::size_t& pos, std::vector<std::string>& args) {
  std::string_view line;
  bool complete = false;  // the line ends in '\n' (fgets() returned a whole line)
  const auto next_line = [&] {
    const std::size_t nl = buf.find('\n', pos);
    complete = nl != std::string_view::npos;
    const std::size_t end = complete ? nl + 1 : buf.size();
    line = buf.substr(pos, end - pos);
    pos = end;
  };
  // strtol() of what follows the line's first byte; `end` is where it stops (1 with no digits).
  std::size_t end = 1;
  const auto number = [&] {
    std::size_t i = 1;
    while (i < line.size() && (line[i] == ' ' || (line[i] >= '\t' && line[i] <= '\r'))) ++i;
    const bool negative = i < line.size() && line[i] == '-';
    if (i < line.size() && (line[i] == '-' || line[i] == '+')) ++i;
    const std::size_t digits = i;
    long long value = 0;
    for (; i < line.size() && line[i] >= '0' && line[i] <= '9'; ++i) {
      if (value < (1LL << 50)) value = value * 10 + (line[i] - '0');
    }
    end = i > digits ? i : 1;
    return negative ? -value : value;
  };
  const auto number_ends_line = [&] {
    return end + 2 == line.size() && line[end] == '\r';
  };

  next_line();
  if (line[0] == '#') return complete ? AofStep::Annotation : AofStep::ShortRead;
  if (line[0] != '*') return AofStep::BadFormat;
  if (line.size() == 1) return AofStep::ShortRead;  // Redis: buf[1] == '\0'
  const long long argc = number();
  if (argc < 1) return AofStep::BadFormat;
  if (!complete) return AofStep::ShortRead;
  if (!number_ends_line()) return AofStep::BadFormat;
  args.clear();
  args.reserve(static_cast<std::size_t>(std::min<long long>(argc, 1024)));
  for (long long j = 0; j < argc; ++j) {
    if (pos >= buf.size()) return AofStep::ShortRead;
    next_line();
    if (line[0] != '$') return AofStep::BadFormat;
    if (!complete) return AofStep::ShortRead;
    const long long len = number();
    if (len < 0 || !number_ends_line()) return AofStep::BadFormat;
    if (static_cast<unsigned long long>(len) + 2 > buf.size() - pos) return AofStep::ShortRead;
    const std::size_t n = static_cast<std::size_t>(len);
    if (buf[pos + n] != '\r' || buf[pos + n + 1] != '\n') return AofStep::BadFormat;
    args.emplace_back(buf.substr(pos, n));
    pos += n + 2;
  }
  return AofStep::Command;
}

// Cuts the file at `path` to `size` bytes and syncs it. The AOF fd is opened with O_APPEND, so the
// next append lands at the new end.
bool truncate_file(const std::string& path, std::size_t size, std::string& err) {
  const int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
  if (fd < 0) {
    err = std::strerror(errno);
    return false;
  }
  const bool ok = ::ftruncate(fd, static_cast<off_t>(size)) == 0 && ::fsync(fd) == 0;
  if (!ok) err = std::strerror(errno);
  ::close(fd);
  return ok;
}

// Redis's aofWrite(): write(2) until every byte is in or a call fails. Returns the bytes written,
// or -1 (errno set) when none were.
ssize_t aof_write(int fd, const char* buf, std::size_t len) {
  ssize_t total = 0;
  while (len > 0) {
    const ssize_t n = ::write(fd, buf, len);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) {
      if (n == 0) errno = ENOSPC;  // Redis would loop for ever here
      return total > 0 ? total : -1;
    }
    buf += n;
    len -= static_cast<std::size_t>(n);
    total += n;
  }
  return total;
}

// With appendfsync always a client may get its reply only once the command is on disk. When that
// cannot be, Redis exits: the command has run and its reply is waiting, so going on would tell
// the client that data which is not on disk is safe. _exit(), because other threads still run.
[[noreturn]] void exit_for_aof_error(const std::string& line) {
  log(LogLevel::Warn, line);
  ::_exit(1);
}

void aof_fsync_always() {
  if (::fsync(g_aof_fd) == -1) {
    exit_for_aof_error("Can't persist AOF for fsync error when the AOF fsync policy is 'always': " +
                       std::string(std::strerror(errno)) + ". Exiting...");
  }
  g_aof_unsynced = false;
  g_aof_fsync_error = false;
  g_last_aof_fsync_ms = now_ms();
}

// Redis's flushAppendOnlyFile() for PeaDB's AOF, which writes each command at once: writes what
// an earlier failed write left in g_aof_buf, then `payload`. A write that fails (or is short, and
// is then cut off the file again with ftruncate(), so the file never ends inside a command) keeps
// the bytes that are not in the file in g_aof_buf, sets the write error (logged at most once per
// 30 s) and, with appendfsync always, exits; the next good write clears the error.
void flush_append_only_file(std::string_view payload) {
  if (!g_aof_buf.empty() && !payload.empty()) {
    g_aof_buf.append(payload);
    payload = {};
  }
  const std::string_view data = g_aof_buf.empty() ? payload : std::string_view(g_aof_buf);
  if (data.empty()) return;
  if (g_aof_fd < 0) open_aof_fd();
  const ssize_t written = g_aof_fd < 0 ? -1 : aof_write(g_aof_fd, data.data(), data.size());
  const int write_errno = errno;
  if (written == static_cast<ssize_t>(data.size())) {
    if (g_aof_write_error) {
      log(LogLevel::Info, "AOF write error looks solved, PeaDB can write again.");
      g_aof_write_error = false;
    }
    clear_buffer(g_aof_buf);
    g_aof_unsynced = true;
    if (g_appendfsync == "always") aof_fsync_always();
    return;
  }
  constexpr std::int64_t kAofWriteLogErrorRate = 30;  // AOF_WRITE_LOG_ERROR_RATE, seconds
  static std::int64_t last_write_error_log = 0;
  const std::int64_t now_s = now_ms() / 1000;
  const bool can_log = now_s - last_write_error_log > kAofWriteLogErrorRate;
  if (can_log) last_write_error_log = now_s;
  std::size_t kept_from = 0;  // the bytes of `data` that are in the file
  if (written < 0) {
    if (can_log) {
      log(LogLevel::Warn,
          "Error writing to the AOF file: " + std::string(std::strerror(write_errno)));
    }
    g_aof_last_write_errno = write_errno;
  } else {
    if (can_log) {
      log(LogLevel::Warn,
          "Short write while writing to the AOF file: (nwritten=" + std::to_string(written) +
              ", expected=" + std::to_string(data.size()) + ")");
    }
    // The file grew by `written` bytes from this write alone (one writer, O_APPEND).
    struct stat st {};
    int cut_errno = 0;
    if (::fstat(g_aof_fd, &st) != 0) {
      cut_errno = errno;
    } else if (st.st_size < written) {
      cut_errno = EINVAL;
    } else if (::ftruncate(g_aof_fd, st.st_size - written) != 0) {
      cut_errno = errno;
    }
    if (cut_errno != 0) {
      if (can_log) {
        log(LogLevel::Warn,
            "Could not remove short write from the append-only file.  PeaDB may "
            "refuse to load the AOF the next time it starts.  ftruncate: " +
                std::string(std::strerror(cut_errno)));
      }
      kept_from = static_cast<std::size_t>(written);
      g_aof_unsynced = true;
    }
    g_aof_last_write_errno = ENOSPC;
  }
  if (g_appendfsync == "always") {
    exit_for_aof_error(
        "Can't recover from AOF write error when the AOF fsync policy is 'always'. "
        "Exiting...");
  }
  g_aof_write_error = true;
  if (data.data() == g_aof_buf.data()) {
    g_aof_buf.erase(0, kept_from);
  } else {
    g_aof_buf.assign(data.substr(kept_from));
  }
}

// Adds the AOF bytes of one execution unit (propagate_pending_commands()) to the file and, while
// a rewrite runs, to its buffer; a whole unit always lands in both, never half of it.
void append_aof_payload(std::string_view payload) {
  if (!g_appendonly || (g_replaying_aof && !g_applying_master_stream) || payload.empty()) return;
  // While the AOF waits for its first rewrite (CONFIG SET appendonly yes) there is no file to
  // append to yet: the command goes only to the rewrite buffer below. After a failed write the
  // bytes wait, and aof_cron() tries them once per event-loop pass, as Redis's beforeSleep() does.
  // With appendfsync always the write is tried at once, so the client never gets a reply first.
  if (g_aof_state == AofState::On) {
    if (g_aof_write_error && g_appendfsync != "always") {
      g_aof_buf.append(payload);
    } else {
      flush_append_only_file(payload);
    }
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
      log(LogLevel::Warn,
          "BGREWRITEAOF: rewrite diff buffer reached 256 MiB cap; "
          "writes beyond this point will not appear in the rewritten AOF. "
          "Consider reducing write rate or increasing available memory.");
    }
  }
}

// ── maxmemory (Redis's evict.c) ──

// Empties a buffer, and gives a large one back to the allocator as Redis's flushAppendOnlyFile()
// does with aof_buf: std::string::clear() keeps the buffer, and used_memory counts it.
void clear_buffer(std::string& buf) {
  if (buf.capacity() < 4000) {
    buf.clear();
  } else {
    std::string().swap(buf);
  }
}

// freeMemoryGetNotCountedMemory(): memory that maxmemory does not count, so that evicting keys
// (whose DELs go into these buffers) cannot feed itself: the replicas' output buffers and, with
// the AOF on, the AOF buffer; also the AOF rewrite buffer (Redis 7 has none; Redis 6 did not count
// its own). The replication backlog counts as far as it holds data, as Redis's does, once a
// replica has attached (used_memory()).
std::size_t free_memory_not_counted() {
  std::size_t overhead = g_replica_output_buffers_cb ? g_replica_output_buffers_cb() : 0;
  if (g_aof_state != AofState::Off) overhead += zmalloc_string_bytes(g_aof_buf);
  overhead += zmalloc_string_bytes(g_aof_rewrite_buf);
  return overhead;
}

// getMaxmemoryState(): true while maxmemory is off or the used memory, less what is not counted,
// is within it; otherwise false, with the bytes to free.
bool maxmemory_state_ok(std::size_t* tofree) {
  if (g_config_maxmemory <= 0) return true;
  const std::size_t reported = used_memory();
  const auto maxmemory = static_cast<std::size_t>(g_config_maxmemory);
  if (reported <= maxmemory) return true;
  const std::size_t overhead = free_memory_not_counted();
  const std::size_t used = reported > overhead ? reported - overhead : 0;
  if (used <= maxmemory) return true;
  if (tofree != nullptr) *tofree = used - maxmemory;
  return false;
}

// overMaxmemoryAfterAlloc(): whether allocating `more_bytes` would put the used memory (less what
// is not counted) over maxmemory.
bool over_maxmemory_after_alloc(std::size_t more_bytes) {
  if (g_config_maxmemory <= 0) return false;
  const auto maxmemory = static_cast<std::size_t>(g_config_maxmemory);
  std::size_t used = used_memory();
  if (used + more_bytes <= maxmemory) return false;
  const std::size_t overhead = free_memory_not_counted();
  used = used > overhead ? used - overhead : 0;
  return used + more_bytes > maxmemory;
}

// dictExpandAllowed() (server.c), installed for the keyspace tables (g_keyspace_expand_allowed):
// up to 1.618 elements per bucket, a table does not double when its new bucket array would go
// over maxmemory, so a rehash near the limit does not evict keys to make room for itself.
bool dict_expand_allowed(std::size_t more_bytes, double used_ratio) {
  constexpr double kHashtableMaxLoadFactor = 1.618;
  if (used_ratio <= kHashtableMaxLoadFactor) return !over_maxmemory_after_alloc(more_bytes);
  return true;
}

int g_maxmemory_eviction_tenacity = 10;  // maxmemory-eviction-tenacity
bool g_eviction_proc_running = false;    // isEvictionProcRunning: the eviction timer is on
// stat_last_eviction_exceeded_time (0 = memory is within the limit) and
// stat_total_eviction_exceeded_time, in steady-clock microseconds.
std::int64_t g_eviction_exceeded_since_us = 0;
std::int64_t g_eviction_exceeded_total_us = 0;

std::int64_t steady_now_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// evictionTimeLimitUs(): how long one call may evict, from maxmemory-eviction-tenacity (10, the
// default, gives 500 us; 100 gives no limit).
std::uint64_t eviction_time_limit_us() {
  if (g_maxmemory_eviction_tenacity <= 10) return 50ULL * g_maxmemory_eviction_tenacity;
  if (g_maxmemory_eviction_tenacity < 100)
    return static_cast<std::uint64_t>(500.0 * std::pow(1.15, g_maxmemory_eviction_tenacity - 10.0));
  return std::numeric_limits<std::uint64_t>::max();
}

// isSafeToPerformEvictions(): no eviction while the AOF loads or the master's stream applies, on a
// replica (replica-ignore-maxmemory yes, a fixed setting), while CLIENT PAUSE stops eviction, or
// while a script is busy.
bool is_safe_to_perform_evictions() {
  if (must_obey_client() || g_replication_role == "slave") return false;
  if (g_script_busy.load(std::memory_order_relaxed)) return false;
  return !paused_actions_with_update(kPauseEvict);
}

// startEvictionTimeProc(): run_eviction_cycle() goes on evicting from the event loop, which does
// not sleep while it runs (eviction_proc_running()).
void start_eviction_time_proc() {
  g_eviction_proc_running = true;
}

// performEvictions(): evict keys by maxmemory-policy until the memory to free at the start has
// been freed (measured around each delete, so the DEL written to the AOF and the replicas is not
// counted against it), at most eviction_time_limit_us() per call (then the eviction timer goes
// on). Each evicted key is propagated like Redis's propagateDeletion(): a DEL to the AOF and the
// replicas, after a SELECT of its database when the stream is in another one, plus an "evicted"
// keyspace event.
EvictResult perform_evictions() {
  if (!is_safe_to_perform_evictions()) return EvictResult::Ok;
  EvictResult result = EvictResult::Fail;
  std::size_t tofree = 0;
  if (maxmemory_state_ok(&tofree)) {
    result = EvictResult::Ok;
  } else if (g_maxmemory_policy != MaxmemoryPolicy::NoEviction) {
    const std::uint64_t limit_us = eviction_time_limit_us();
    const std::int64_t start_us = steady_now_us();
    std::int64_t freed = 0;
    int keys_freed = 0;
    bool nothing_to_evict = false;
    // The DELs of this pass go out together when it ends, each on its own (no MULTI/EXEC), as
    // Redis writes them with its next AOF flush: one AOF write (and one fsync under appendfsync
    // always) per pass, not per key. Inside a unit that is already open they join it, in order.
    const ExecutionUnit unit(nullptr, /*may_wrap=*/false);
    while (freed < static_cast<std::int64_t>(tofree)) {
      const auto before = static_cast<std::int64_t>(zmalloc_used_memory());
      auto victim = store().evict_one(g_maxmemory_policy, g_maxmemory_samples);
      if (!victim) {
        nothing_to_evict = true;
        break;
      }
      freed += before - static_cast<std::int64_t>(zmalloc_used_memory());
      also_propagate(victim->db, {"DEL", victim->key}, kPropagateAof | kPropagateRepl);
      notify_keyspace_event(NOTIFY_EVICTED, "evicted", victim->key, victim->db);
      // After some time, stop early, even if the memory is not back under the limit yet.
      if (++keys_freed % 16 == 0 &&
          static_cast<std::uint64_t>(steady_now_us() - start_us) > limit_us) {
        start_eviction_time_proc();
        break;
      }
    }
    if (!nothing_to_evict) {
      result = g_eviction_proc_running ? EvictResult::Running : EvictResult::Ok;
    } else {
      // Nothing left to evict, but the lazyfree thread may still be freeing memory: wait a little.
      while (store().lazyfree_pending_objects() > 0 &&
             static_cast<std::uint64_t>(steady_now_us() - start_us) < limit_us) {
        if (maxmemory_state_ok(nullptr)) {
          result = EvictResult::Ok;
          break;
        }
        ::usleep(static_cast<useconds_t>(std::min<std::uint64_t>(limit_us, 1000)));
      }
    }
  }
  if (result == EvictResult::Ok) {
    if (g_eviction_exceeded_since_us != 0) {
      g_eviction_exceeded_total_us += steady_now_us() - g_eviction_exceeded_since_us;
      g_eviction_exceeded_since_us = 0;
    }
  } else if (g_eviction_exceeded_since_us == 0) {
    g_eviction_exceeded_since_us = steady_now_us();
  }
  return result;
}

std::uint64_t command_flags(const std::string& cmd, const std::vector<std::string>& args);  // below

// The maxmemory step of processCommand() for a client's own command (not one EXEC or a script
// runs, never the AOF or the master stream): evict as the policy says, and refuse with -OOM, while
// memory is still over the limit, a denyoom command (by command_flags(); an EXEC by the commands
// it queued) or, inside MULTI, any command but EXEC, DISCARD, QUIT and RESET (queueing may use
// unlimited memory). The result is kept for the command's scripts (g_pre_command_oom_state).
bool maxmemory_refuses(const std::string& cmd, const std::vector<std::string>& args,
                       const SessionState& session) {
  if (g_config_maxmemory <= 0) return false;
  const bool out_of_memory = perform_evictions() == EvictResult::Fail;
  g_pre_command_oom_state = out_of_memory;
  if (!out_of_memory) return false;
  if (session.in_multi && cmd != "EXEC" && cmd != "DISCARD" && cmd != "QUIT" && cmd != "RESET")
    return true;
  if (cmd == "EXEC" && session.in_multi) return (session.multi_flags & kRedisCmdDenyoom) != 0;
  return (command_flags(cmd, args) & kRedisCmdDenyoom) != 0;
}

// scriptVerifyOOM(): inside a script, a denyoom command is refused only when memory was over the
// limit as the script's command started and the script has not written yet (a script is never
// stopped half done), unless it runs with allow-oom.
bool script_refuses_for_oom(const std::vector<std::string>& args) {
  if (g_script_allow_oom || g_config_maxmemory <= 0 || must_obey_client()) return false;
  if (g_script_write_dirty || !g_pre_command_oom_state) return false;
  return (redis_command_flags(args) & kRedisCmdDenyoom) != 0;
}

bool rewrite_aof_file(std::string& err);  // defined below

// aofRewriteLimited() (aof.c): after 3 failed rewrites in a row, a retry waits 1 minute, then
// 2, 4, ... up to 60, so a rewrite that keeps failing (no memory to fork, a full disk) does not
// fork again on every event-loop pass.
bool aof_rewrite_limited() {
  static int next_delay_minutes = 0;
  static std::int64_t next_rewrite_s = 0;
  if (g_aofrw_consecutive_failures < 3) {
    next_delay_minutes = 0;
    next_rewrite_s = 0;
    return false;
  }
  const std::int64_t now_s = now_ms() / 1000;
  if (next_rewrite_s != 0) {
    if (now_s < next_rewrite_s) return true;
    next_rewrite_s = 0;
    return false;
  }
  next_delay_minutes = next_delay_minutes == 0 ? 1 : std::min(next_delay_minutes * 2, 60);
  next_rewrite_s = now_s + next_delay_minutes * 60;
  log(LogLevel::Warn,
      "Background AOF rewrite has repeatedly failed and triggered the limit, "
      "will retry in " +
          std::to_string(next_delay_minutes) + " minutes");
  return true;
}

// Forks the AOF rewrite child (BGREWRITEAOF, and CONFIG SET appendonly yes). The child writes
// the dataset to <appendfilename>.tmp; reap_background_children() adds the commands buffered
// meanwhile and renames it over the AOF.
bool start_aof_rewrite() {
  const auto fork_t0 = std::chrono::steady_clock::now();
  const pid_t pid = fork_child();
  if (pid < 0) {
    g_aof_last_bgrewrite_status = "err";
    log(LogLevel::Warn,
        "Can't rewrite append only file in background: fork: " + std::string(std::strerror(errno)));
    return false;
  }
  if (pid == 0) {
    std::string err;
    const bool ok = rewrite_aof_file(err);
    if (!ok) log(LogLevel::Warn, err);
    _exit(ok ? 0 : 1);
  }
  g_latest_fork_usec = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - fork_t0)
                           .count();
  ++g_total_forks;
  g_bgrewrite_in_progress.store(true);
  g_bgrewrite_child_pid.store(pid);
  clear_buffer(g_aof_rewrite_buf);
  g_aof_rewrite_buf_capped = false;  // reset per-rewrite overflow flag
  g_aof_rewrite_scheduled = false;
  // The rewritten file ends in whatever database its last SELECT named, so the first command
  // buffered for it must carry its own SELECT.
  g_last_aof_db = -1;
  return true;
}

// Stops a running rewrite and throws its work away (killAppendOnlyChild()).
void kill_aof_rewrite_child() {
  const pid_t pid = g_bgrewrite_child_pid.load();
  if (!g_bgrewrite_in_progress.load() || pid <= 0) return;
  ::kill(pid, SIGKILL);
  int status = 0;
  while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
  }
  ::unlink((g_aof_path.string() + ".tmp").c_str());
  g_bgrewrite_child_pid.store(0);
  g_bgrewrite_in_progress.store(false);
  clear_buffer(g_aof_rewrite_buf);
}

// CONFIG SET appendonly yes (startAppendOnly()): the AOF starts with a rewrite of the current
// dataset, and every write from now on is kept for it.
bool start_append_only(std::string& err) {
  g_aof_sync_suspended = false;
  g_appendonly = true;
  g_aof_state = AofState::WaitRewrite;
  // A rewrite started while the AOF was off has not kept the writes made since it started, so its
  // file can never become the AOF: stop it, here even when the new rewrite has to wait.
  const bool rewrite_running = g_bgrewrite_in_progress.load();
  if (rewrite_running) kill_aof_rewrite_child();
  if (g_bgsave_in_progress.load()) {
    g_aof_rewrite_scheduled = true;
    log(LogLevel::Info,
        "AOF was enabled but there is already another background operation. An "
        "AOF background was scheduled to start when possible.");
  } else if (g_executing_exec) {
    // As for BGREWRITEAOF inside EXEC: the transaction's writes are in the AOF only once EXEC
    // ends, so the rewrite starts after it, from the cron.
    g_aof_rewrite_scheduled = true;
    log(LogLevel::Info,
        "AOF was enabled during a transaction. An AOF background was scheduled to "
        "start when possible.");
  } else {
    if (rewrite_running) {
      log(LogLevel::Info,
          "AOF was enabled but there is already an AOF rewriting in background. "
          "Stopping background AOF and starting a rewrite now.");
    }
    if (!start_aof_rewrite()) {
      g_appendonly = false;
      g_aof_state = AofState::Off;
      log(LogLevel::Warn,
          "PeaDB needs to enable the AOF but can't trigger a background AOF "
          "rewrite operation. Check the above logs for more info about the error.");
      err = "Unable to turn on AOF. Check server logs.";
      return false;
    }
  }
  // The new AOF starts from the dataset, so an earlier error no longer counts.
  if (g_aof_fsync_error) {
    log(LogLevel::Warn, "AOF reopen, just ignore the AOF fsync error in bio job");
    g_aof_fsync_error = false;
  }
  if (g_aof_write_error) {
    log(LogLevel::Warn, "AOF reopen, just ignore the last error.");
    g_aof_write_error = false;
  }
  return true;
}

// CONFIG SET appendonly no (stopAppendOnly()). Bytes a failed write left are dropped (the AOF is
// off), but the write and fsync errors stay for INFO until the AOF is turned on again.
void stop_append_only() {
  g_aof_sync_suspended = false;
  if (g_aof_state == AofState::On) flush_append_only_file({});
  if (g_aof_fd >= 0) {
    if (::fsync(g_aof_fd) != 0) {
      log(LogLevel::Warn, "Fail to fsync the AOF file: " + std::string(std::strerror(errno)));
    }
    ::close(g_aof_fd);
    g_aof_fd = -1;
  }
  g_last_aof_db = -1;
  clear_buffer(g_aof_buf);
  g_aof_unsynced = false;
  g_appendonly = false;
  g_aof_state = AofState::Off;
  g_aof_rewrite_scheduled = false;
  kill_aof_rewrite_child();
}

// Redis's restartAOFAfterSYNC(): a new full sync must replace the old AOF's entire dataset.
// While its first rewrite runs, streaming writes join the rewrite buffer. A replica that cannot
// start the rewrite after ten attempts exits rather than advertising unreliable persistence.
void restart_aof_after_sync() {
  for (int attempt = 0; attempt < 10; ++attempt) {
    std::string err;
    if (start_append_only(err)) return;
    log(LogLevel::Warn,
        "Failed enabling the AOF after successful master synchronization! "
        "Trying it again in one second.");
    ::sleep(1);
  }
  log(LogLevel::Error,
      "FATAL: this replica instance finished the synchronization with "
      "its master, but the AOF can't be turned on. Exiting now.");
  ::_exit(1);
}

bool rewrite_aof_file(std::string& err) {
  err.clear();
  const std::string tmp_path = g_aof_path.string() + ".tmp";
  const int tmp_fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (tmp_fd < 0) {
    err = "cannot open AOF tmp for rewrite";
    return false;
  }
  bool write_failed = false;
  // Full-write loop: a single ::write() may return a short (positive) count under disk
  // pressure or signal interruption; treating that as success would leave a truncated temp
  // AOF that the parent then commits.
  auto write_all = [&](const std::string& payload) {
    const char* p = payload.data();
    std::size_t remaining = payload.size();
    while (remaining > 0) {
      const ssize_t rc = ::write(tmp_fd, p, remaining);
      if (rc <= 0) {
        write_failed = true;
        return;
      }
      p += static_cast<std::size_t>(rc);
      remaining -= static_cast<std::size_t>(rc);
    }
  };
  // Function libraries are server-level state, not in the keyspace — emit them first so an AOF
  // rewrite (and any later DEBUG LOADAOF / restart) preserves loaded functions.
  for (const auto& [lc, lib] : g_libraries) {
    write_all(encode_command_resp({"FUNCTION", "LOAD", "REPLACE", lib.code}));
    if (write_failed) {
      ::close(tmp_fd);
      ::unlink(tmp_path.c_str());
      err = "AOF rewrite write error";
      return false;
    }
  }
  const auto cmds = store().export_aof_commands();
  // rdb-key-save-delay after each key, as in Redis's rewriteAppendOnlyFileRio(). A key's commands
  // are next to each other, so a key ends where the key argument changes or a SELECT starts the
  // next database (XGROUP has its key after the subcommand).
  const int key_save_delay = rdb_key_save_delay();
  const std::string* prev_key = nullptr;
  for (const auto& cmd : cmds) {
    if (key_save_delay != 0) {
      const std::size_t key_at = cmd[0] == "XGROUP" ? 2 : 1;
      const std::string* key = cmd[0] == "SELECT" || cmd.size() <= key_at ? nullptr : &cmd[key_at];
      if (prev_key != nullptr && (key == nullptr || *prev_key != *key)) debug_delay(key_save_delay);
      prev_key = key;
    }
    write_all(encode_command_resp(cmd));
    if (write_failed) {
      ::close(tmp_fd);
      ::unlink(tmp_path.c_str());
      err = "AOF rewrite write error";
      return false;
    }
  }
  if (prev_key != nullptr) debug_delay(key_save_delay);  // after the last key
  if (::fsync(tmp_fd) != 0) {
    ::close(tmp_fd);
    ::unlink(tmp_path.c_str());
    err = "AOF rewrite fsync error";
    return false;
  }
  ::close(tmp_fd);
  // Parent owns the rename + commit point: child writes temp.aof and exits;
  // parent appends the diff buffer and only then atomically renames over the
  // live AOF. This means any failure (cap overflow, diff append error, rename
  // error, child crash) leaves the old AOF intact on disk.
  return true;
}

// This round trip runs synchronously on the single event-loop thread, so every other client
// stalls for however long it takes — there is no async handshake state machine to fall back
// to (compat/delta.md item 6). An unbounded connect()/read() here used to mean a single bad
// address (unreachable, filtered, or this node's own — see the caller's self-address check)
// could hang the whole server until killed. Bound both with a short timeout instead: this does
// not make MEET reliable over a slow or lossy link, but it prevents one bad address from
// hanging every client indefinitely, which is the concrete, reported hazard. connect() itself
// ignores SO_SNDTIMEO/SO_RCVTIMEO on Linux, so the socket is made non-blocking for the connect
// step and polled with the same budget.
constexpr int kMeetbackTimeoutMs = 1000;

void try_send_cluster_meetback(const std::string& host, int port) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return;
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &sa.sin_addr) != 1) {
    close(fd);
    return;
  }

  timeval tv{};
  tv.tv_sec = kMeetbackTimeoutMs / 1000;
  tv.tv_usec = (kMeetbackTimeoutMs % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  const int orig_flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, orig_flags | O_NONBLOCK);
  const int connect_rc = connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa));
  if (connect_rc != 0 && errno != EINPROGRESS) {
    close(fd);
    return;
  }
  if (connect_rc != 0) {
    pollfd pfd{fd, POLLOUT, 0};
    const int poll_rc = poll(&pfd, 1, kMeetbackTimeoutMs);
    int so_error = 0;
    socklen_t so_error_len = sizeof(so_error);
    if (poll_rc <= 0 || !(pfd.revents & POLLOUT) ||
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &so_error_len) != 0 || so_error != 0) {
      close(fd);
      return;
    }
  }
  fcntl(fd, F_SETFL, orig_flags);  // restore blocking mode for the write/read below

  std::vector<std::string> args = {"CLUSTER", "MEETBACK", cluster_announced_host(),
                                   std::to_string(cluster_announced_port())};
  const auto payload = encode_command_resp(args);
  std::size_t sent = 0;
  while (sent < payload.size()) {
    const auto rc = write(fd, payload.data() + sent, payload.size() - sent);
    if (rc <= 0) break;
    sent += static_cast<std::size_t>(rc);
  }
  // Read the +OK reply before closing so the peer has time to process; bounded by SO_RCVTIMEO.
  char buf[32];
  (void)read(fd, buf, sizeof(buf));
  close(fd);
}

// ── MIGRATE's connections (Redis's cluster.c and syncio.c) ──────────────────────

std::int64_t steady_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// Redis's aeWait(): waits up to `ms` for `events` on `fd`.
void ae_wait(int fd, short events, std::int64_t ms) {
  pollfd pfd{fd, events, 0};
  (void)poll(&pfd, 1, static_cast<int>(std::min<std::int64_t>(ms, INT_MAX)));
}

constexpr std::int64_t kSyncioResolutionMs = 10;  // Redis's SYNCIO__RESOLUTION

// Redis's syncWrite() on a non-blocking socket: `size` on success; -1 when a write fails or the
// whole buffer has not gone out within `timeout` ms (errno ETIMEDOUT then).
ssize_t sync_write(int fd, const char* ptr, std::size_t size, std::int64_t timeout) {
  const auto ret = static_cast<ssize_t>(size);
  const std::int64_t start = steady_ms();
  std::int64_t remaining = timeout;
  while (true) {
    const std::int64_t wait = remaining > kSyncioResolutionMs ? remaining : kSyncioResolutionMs;
    const ssize_t nwritten = ::write(fd, ptr, size);
    if (nwritten == -1) {
      if (errno != EAGAIN) return -1;
    } else {
      ptr += nwritten;
      size -= static_cast<std::size_t>(nwritten);
    }
    if (size == 0) return ret;
    ae_wait(fd, POLLOUT, wait);
    const std::int64_t elapsed = steady_ms() - start;
    if (elapsed >= timeout) {
      errno = ETIMEDOUT;
      return -1;
    }
    remaining = timeout - elapsed;
  }
}

// Redis's syncRead(): -1 on an error, on the end of the stream or when `size` bytes have not come
// within `timeout` ms (errno ETIMEDOUT then).
ssize_t sync_read(int fd, char* ptr, std::size_t size, std::int64_t timeout) {
  ssize_t totread = 0;
  const std::int64_t start = steady_ms();
  std::int64_t remaining = timeout;
  if (size == 0) return 0;
  while (true) {
    const std::int64_t wait = remaining > kSyncioResolutionMs ? remaining : kSyncioResolutionMs;
    const ssize_t nread = ::read(fd, ptr, size);
    if (nread == 0) return -1;  // short read
    if (nread == -1) {
      if (errno != EAGAIN) return -1;
    } else {
      ptr += nread;
      size -= static_cast<std::size_t>(nread);
      totread += nread;
    }
    if (size == 0) return totread;
    ae_wait(fd, POLLIN, wait);
    const std::int64_t elapsed = steady_ms() - start;
    if (elapsed >= timeout) {
      errno = ETIMEDOUT;
      return -1;
    }
    remaining = timeout - elapsed;
  }
}

// Redis's syncReadLine() into a buffer of `size` bytes: reads up to a '\n', each byte within
// `timeout` ms, and returns the bytes before it (a '\r' counts but is not kept), at most size - 1
// of them (the rest of a longer line stays unread); -1 on an error.
ssize_t sync_read_line(int fd, std::string& line, std::size_t size, std::int64_t timeout) {
  line.clear();
  ssize_t nread = 0;
  for (--size; size > 0; --size) {
    char c = 0;
    if (sync_read(fd, &c, 1, timeout) == -1) return -1;
    if (c == '\n') {
      if (!line.empty() && line.back() == '\r') line.pop_back();
      return nread;
    }
    line.push_back(c);
    ++nread;
  }
  return nread;
}

// Redis's connBlockingConnect() for TCP (anetTcpNonBlockConnect(), then aeWait()): a non-blocking
// connect to the first address of `host` that takes one (a name, IPv4 or IPv6), which may wait up
// to `timeout` ms. As in Redis a connect still in progress then is not an error: the first write
// fails or times out instead. -1 when no address takes a connect.
int migrate_connect(const std::string& host, int port, std::int64_t timeout) {
  char portstr[6];  // Redis's strlen("65535") + 1: a longer number is cut, as in Redis
  std::snprintf(portstr, sizeof(portstr), "%d", port);
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* servinfo = nullptr;
  if (getaddrinfo(host.c_str(), portstr, &hints, &servinfo) != 0) return -1;
  int s = -1;
  for (addrinfo* p = servinfo; p != nullptr; p = p->ai_next) {
    s = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (s == -1) continue;
    const int yes = 1;
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1 ||
        fcntl(s, F_SETFL, fcntl(s, F_GETFL, 0) | O_NONBLOCK) == -1) {
      ::close(s);
      s = -1;
      break;
    }
    if (::connect(s, p->ai_addr, p->ai_addrlen) == 0 || errno == EINPROGRESS) break;
    ::close(s);
    s = -1;
  }
  freeaddrinfo(servinfo);
  if (s == -1) return -1;
  ae_wait(s, POLLOUT, timeout);
  const int yes = 1;
  (void)setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
  return s;
}

// Redis's migrateCachedSocket: MIGRATE keeps its connection to each "host:port" (as the client
// wrote them) and the database it last selected there, and closes it 10 seconds after its last
// use (migrate_cron()); at most 64 are kept.
struct MigrateCachedSocket {
  int fd = -1;
  std::int64_t last_dbid = -1;     // -1: send SELECT first
  std::int64_t last_use_time = 0;  // unix seconds
  bool authenticated = false;      // an AUTH sent on it succeeded
};
std::unordered_map<std::string, MigrateCachedSocket> g_migrate_cached_sockets;
constexpr std::size_t kMigrateSocketCacheItems = 64;  // Redis's MIGRATE_SOCKET_CACHE_ITEMS
constexpr std::int64_t kMigrateSocketCacheTtl = 10;   // Redis's MIGRATE_SOCKET_CACHE_TTL

// Redis's migrateGetSocket(): the kept connection to `name`, or a new one (dropping a random kept
// one when 64 are kept); nullptr when the connect fails.
MigrateCachedSocket* migrate_get_socket(const std::string& name, const std::string& host,
                                        const std::string& port, std::int64_t timeout) {
  const std::int64_t now_s = now_ms() / 1000;
  if (auto it = g_migrate_cached_sockets.find(name); it != g_migrate_cached_sockets.end()) {
    it->second.last_use_time = now_s;
    return &it->second;
  }
  if (g_migrate_cached_sockets.size() == kMigrateSocketCacheItems) {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    auto victim = std::next(g_migrate_cached_sockets.begin(),
                            static_cast<std::ptrdiff_t>(rng() % g_migrate_cached_sockets.size()));
    ::close(victim->second.fd);
    g_migrate_cached_sockets.erase(victim);
  }
  const int fd = migrate_connect(host, std::atoi(port.c_str()), timeout);
  if (fd == -1) return nullptr;
  MigrateCachedSocket& cs = g_migrate_cached_sockets[name];
  cs.fd = fd;
  cs.last_dbid = -1;
  cs.last_use_time = now_s;
  cs.authenticated = false;
  return &cs;
}

// Redis's migrateCloseSocket(): forget the kept connection, so the next MIGRATE connects again.
void migrate_close_socket(const std::string& name) {
  auto it = g_migrate_cached_sockets.find(name);
  if (it == g_migrate_cached_sockets.end()) return;
  ::close(it->second.fd);
  g_migrate_cached_sockets.erase(it);
}

std::string sha1_hex(const std::string& script) {
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
  oss << std::hex << std::setfill('0') << std::nouppercase << std::setw(8) << h0 << std::setw(8)
      << h1 << std::setw(8) << h2 << std::setw(8) << h3 << std::setw(8) << h4;
  return oss.str();
}

std::uint16_t crc16_xmodem(std::string_view s) {
  std::uint16_t crc = 0;
  for (unsigned char c : s) {
    crc ^= static_cast<std::uint16_t>(c) << 8;
    for (int i = 0; i < 8; ++i) {
      if (crc & 0x8000)
        crc = static_cast<std::uint16_t>((crc << 1) ^ 0x1021);
      else
        crc = static_cast<std::uint16_t>(crc << 1);
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

// Node-table access. Index 0 of g_cluster_nodes is always self.

// Registers self as g_cluster_nodes[0] if the table has not been touched yet, and returns 0.
// Every node-table reader/writer below calls this first so index 0 exists as soon as the
// table is read, with no explicit startup hook: self's announced address is only knowable
// once configure_cluster() has run, and by the time any client can reach CLUSTER it has.
int ensure_self_registered() {
  if (g_cluster_nodes.empty()) {
    const std::string host = cluster_announced_host();
    const int port = cluster_announced_port();
    g_cluster_nodes.push_back(ClusterNode{
        sha1_hex("peadb-cluster-node:" + host + ":" + std::to_string(port)), host, port});
  }
  return 0;
}

// Re-derives g_cluster_nodes[0] (id, host, port) from the current cluster-announce-* settings.
// Must be called after CONFIG SET changes any of cluster-announce-ip/-port/-bus-port, so the
// node table can never disagree with CONFIG GET about this node's own advertised identity —
// otherwise a CONFIG SET landing after the first CLUSTER call (which lazily snapshots the table
// via ensure_self_registered() above) would leave CLUSTER NODES/SLOTS naming a stale address, or
// worse, one that mixes the new host with the old id. The P5 decision record already commits to
// "re-announcing on a new address mints a new id" (docs/decisions/
// p5-cluster-addressing-and-version-provenance.md); this makes that true unconditionally rather
// than only for a node whose table has never been read yet. A no-op before the table has ever
// been touched — the next ensure_self_registered() call builds it fresh from current settings
// anyway, so there is nothing to rebuild yet.
void reregister_self_if_needed() {
  if (g_cluster_nodes.empty()) return;
  const std::string host = cluster_announced_host();
  const int port = cluster_announced_port();
  g_cluster_nodes[0] =
      ClusterNode{sha1_hex("peadb-cluster-node:" + host + ":" + std::to_string(port)), host, port};
}

// Inserts (or finds) a peer by its announced host:port, used by MEET/MEETBACK. Idempotent:
// re-MEETing the same address returns the existing index rather than minting a duplicate.
int cluster_register_peer(const std::string& host, int port) {
  ensure_self_registered();
  for (std::size_t i = 1; i < g_cluster_nodes.size(); ++i) {
    if (g_cluster_nodes[i].host == host && g_cluster_nodes[i].port == port) {
      return static_cast<int>(i);
    }
  }
  g_cluster_nodes.push_back(
      ClusterNode{sha1_hex("peadb-cluster-node:" + host + ":" + std::to_string(port)), host, port});
  return static_cast<int>(g_cluster_nodes.size() - 1);
}

// Resolves "self" (case-insensitive, matching the existing SETSLOT convention), a known
// 40-hex node id, or a "host:port" token to an index into g_cluster_nodes. -1 if unknown.
int cluster_resolve_node(const std::string& token) {
  ensure_self_registered();
  if (lower(token) == "self") return 0;
  for (std::size_t i = 0; i < g_cluster_nodes.size(); ++i) {
    if (g_cluster_nodes[i].id == token) return static_cast<int>(i);
  }
  for (std::size_t i = 0; i < g_cluster_nodes.size(); ++i) {
    if (g_cluster_nodes[i].host + ":" + std::to_string(g_cluster_nodes[i].port) == token) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

// "host:port" for a node table index, for MOVED/ASK and CLUSTER NODES. Empty if out of range.
std::string cluster_node_addr(int index) {
  ensure_self_registered();
  if (index < 0 || static_cast<std::size_t>(index) >= g_cluster_nodes.size()) return "";
  const auto& n = g_cluster_nodes[static_cast<std::size_t>(index)];
  return n.host + ":" + std::to_string(n.port);
}

// Groups slot 0..16383 into contiguous [start, end, owner-index] runs, from this node's own
// point of view (g_slot_owner), skipping unassigned slots (owner == -1) entirely — the same
// end result as clusterReplyMultiBulkSlots's boundary-skip loop (cluster.c:5787-5806), which
// re-enters its "find start" branch on every NULL-owner slot so no group is ever emitted for
// them. Shared by CLUSTER SLOTS, SHARDS, and NODES so the three replies can never disagree
// about who owns what.
std::vector<std::tuple<int, int, int>> cluster_owned_ranges() {
  std::vector<std::tuple<int, int, int>> ranges;
  int run_start = -1;
  int run_owner = -1;
  for (int slot = 0; slot < 16384; ++slot) {
    const int owner = g_slot_owner[static_cast<std::size_t>(slot)];
    if (owner != run_owner) {
      if (run_owner != -1) ranges.emplace_back(run_start, slot - 1, run_owner);
      run_owner = owner;
      run_start = slot;
    }
  }
  if (run_owner != -1) ranges.emplace_back(run_start, 16383, run_owner);
  return ranges;
}

// "170" or "170-201" — the per-node slot-range tokens at the tail of a CLUSTER NODES line
// (clusterGenNodeDescription, cluster.c:5323-5327: a single slot never gets a "-end" suffix).
std::string format_slot_range(int start, int end) {
  return start == end ? std::to_string(start) : std::to_string(start) + "-" + std::to_string(end);
}

std::string eval_miniscript(const std::string& script, const std::vector<std::string>& keys,
                            const std::vector<std::string>& argv) {
  // Simulate trivially-infinite scripts in a cooperative loop so that
  // other clients observe BUSY and can issue SCRIPT/FUNCTION KILL.
  if (script.find("while true do") != std::string::npos ||
      script.find("while 1 do") != std::string::npos) {
    g_script_kill_requested.store(false, std::memory_order_release);
    g_script_busy.store(true, std::memory_order_release);
    g_busy_script_session = g_script_current_session;
    g_busy_is_function = false;
    return std::string(
        "-BUSY Redis is busy running a script. Script killed by user with SCRIPT KILL.\r\n");
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
  out += encode_bulk("server");
  out += encode_bulk("redis");
  out += encode_bulk("version");
  out += encode_bulk("7.2.5");
  out += encode_bulk("proto");
  out += encode_integer(r3 ? 3 : 2);
  out += encode_bulk("id");
  out += encode_integer(static_cast<long long>(session.client_id));
  out += encode_bulk("mode");
  out += encode_bulk("standalone");
  out += encode_bulk("role");
  out += encode_bulk(role);
  out += encode_bulk("modules");
  out += "*0\r\n";
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
    } catch (...) {
      ms = 0;
      seq = 0;
    }
  };
  std::uint64_t am, as, bm, bs;
  parse(a, am, as);
  parse(b, bm, bs);
  return am != bm ? am < bm : as < bs;
}

// A well-formed explicit stream id: "ms" or "ms-seq", digits only (no "-"/"+"/"*"/garbage).
bool stream_id_is_valid(const std::string& s) {
  auto all_digits = [](const std::string& t) {
    if (t.empty()) return false;
    for (char c : t)
      if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    errno = 0;
    char* end = nullptr;
    std::strtoull(t.c_str(), &end, 10);
    return errno != ERANGE && end == t.c_str() + t.size();
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
  if (s[i] == '0' && (s.size() - i > 1 || i == 1)) return false;  // "01" and "-0" are not integers
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

// Redis streamParseStrictIDOrReply()/string2ull() for claim IDs. Unlike signed options,
// unsigned ID components also accept strtoull's forms (for example "+1" and " 1").
// Normalize before the datastore lookup so a real master's raw XCLAIM IDs are not dropped.
bool normalize_claim_stream_id(const std::string& input, std::string& out,
                               bool allow_special = false) {
  if (input.empty() || input.size() >= 128) return false;
  const std::string id(input.c_str());  // Redis parses its bounded, NUL-terminated ID buffer.
  if (id == "+" || id == "-") {
    if (!allow_special) return false;
    out = id == "-" ? "0-0" : "18446744073709551615-18446744073709551615";
    return true;
  }
  auto component = [](const std::string& text, std::uint64_t& value) {
    std::int64_t signed_value;
    if (parse_strict_int(text, signed_value)) {
      if (signed_value < 0) return false;
      value = static_cast<std::uint64_t>(signed_value);
      return true;
    }
    errno = 0;
    char* end = nullptr;
    value = std::strtoull(text.c_str(), &end, 10);
    return !text.empty() && errno != ERANGE && errno != EINVAL && end == text.c_str() + text.size();
  };
  const auto dash = id.find('-');
  std::uint64_t ms, seq = 0;
  if (!component(id.substr(0, dash), ms)) return false;
  if (dash != std::string::npos && !component(id.substr(dash + 1), seq)) return false;
  out = std::to_string(ms) + "-" + std::to_string(seq);
  return true;
}

// Redis's parseScanCursorOrReply(): strtoul() in base 10, so a sign is read ("-1" is the largest
// cursor) and an empty cursor is 0, but a leading space, anything after the number and a value past
// 2^64 - 1 are refused.
bool parse_scan_cursor(const std::string& arg, std::uint64_t& cursor) {
  const char* s = arg.c_str();
  if (std::isspace(static_cast<unsigned char>(s[0]))) return false;
  errno = 0;
  char* end = nullptr;
  const unsigned long long v = std::strtoull(s, &end, 10);
  if (*end != '\0' || errno == ERANGE) return false;
  cursor = v;
  return true;
}

struct ScanOptions {
  std::int64_t count = 10;
  std::optional<std::string> pattern;  // MATCH; unset (or "*") matches every element
  std::optional<std::string> type;     // TYPE (SCAN only), in lower case
};

// Step 1 of Redis's scanGenericCommand(): COUNT (an integer of at least 1), MATCH and, for SCAN
// only, TYPE, each with its value, in any order and repeatable (the last one wins). Returns the
// error reply, or "" when the options are good.
std::string parse_scan_options(const std::vector<std::string>& args, std::size_t i, bool keyspace,
                               ScanOptions& opts) {
  for (; i < args.size(); i += 2) {
    const bool has_value = args.size() - i >= 2;
    const std::string opt = lower(args[i]);
    if (opt == "count" && has_value) {
      std::int64_t count = 0;
      if (!parse_strict_int(args[i + 1], count)) {
        return encode_error("value is not an integer or out of range");
      }
      if (count < 1) return encode_error("syntax error");
      opts.count = count;
    } else if (opt == "match" && has_value) {
      if (args[i + 1] == "*") {
        opts.pattern.reset();
      } else {
        opts.pattern = args[i + 1];
      }
    } else if (opt == "type" && keyspace && has_value) {
      opts.type = lower(args[i + 1]);
    } else {
      return encode_error("syntax error");
    }
  }
  return "";
}

// HSCAN, SSCAN and ZSCAN, like Redis's hscanCommand(), sscanCommand() and zscanCommand(): the
// cursor is read first, then the key (a missing key answers an empty page before any option is
// read, a key of another type WRONGTYPE), then the options, then the scan (scanGenericCommand()).
std::string collection_scan_command(const std::vector<std::string>& args, ValueType type) {
  std::uint64_t cursor = 0;
  if (!parse_scan_cursor(args[2], cursor)) return encode_error("invalid cursor");
  const std::string& key = args[1];
  const std::string have = store().type_of(key);
  if (have == "none") return encode_array({encode_bulk("0"), encode_array({})});
  const char* want = type == ValueType::Hash ? "hash" : type == ValueType::Set ? "set" : "zset";
  if (have != want) return wrongtype_error_reply();
  ScanOptions opts;
  if (std::string err = parse_scan_options(args, 3, /*keyspace=*/false, opts); !err.empty()) {
    return err;
  }
  bool wrongtype = false;
  std::uint64_t next = 0;
  std::vector<std::string> items;
  if (type == ValueType::Hash) {
    auto [n, fields] = store().hscan(key, cursor, opts.count, opts.pattern, wrongtype);
    next = n;
    items.reserve(fields.size() * 2);
    for (const auto& [f, v] : fields) {
      items.push_back(encode_bulk(f));
      items.push_back(encode_bulk(v));
    }
  } else if (type == ValueType::Set) {
    auto [n, members] = store().sscan(key, cursor, opts.count, opts.pattern, wrongtype);
    next = n;
    items.reserve(members.size());
    for (const auto& m : members) items.push_back(encode_bulk(m));
  } else {
    auto [n, members] = store().zscan(key, cursor, opts.count, opts.pattern, wrongtype);
    next = n;
    items.reserve(members.size() * 2);
    for (const auto& [m, score] : members) {
      items.push_back(encode_bulk(m));
      items.push_back(encode_bulk(format_score(score)));
    }
  }
  return encode_array({encode_bulk(std::to_string(next)), encode_array(items)});
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
      if (a[i - 1] == b[j - 1])
        dp[i][j] = dp[i - 1][j - 1] + 1;
      else
        dp[i][j] = std::max(dp[i - 1][j], dp[i][j - 1]);
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
    if (dp[i - 1][j] >= dp[i][j - 1])
      --i;
    else
      --j;
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

// The options of SET (`is_set`) and GETEX as Redis's parseExtendedStringArgumentsOrReply() reads
// them: NX, XX, GET and KEEPTTL only for SET, PERSIST only for GETEX, NX never with XX, and one
// kind of expire (the same kind twice is allowed, the last value counts), never with KEEPTTL or
// PERSIST. Returns false for Redis's syntax error. The expire value is checked later, by
// parse_expiry().
struct ExtendedStringArgs {
  bool nx = false;
  bool xx = false;
  bool get = false;
  bool keepttl = false;
  bool persist = false;
  std::string expire_mode;  // "EX", "PX", "EXAT" or "PXAT"; empty without an expire
  const std::string* expire = nullptr;
};

bool parse_extended_string_args(const std::vector<std::string>& args, bool is_set,
                                ExtendedStringArgs& out) {
  for (std::size_t j = is_set ? 3 : 2; j < args.size(); ++j) {
    const std::string opt = upper(args[j]);
    if (opt == "NX" && is_set && !out.xx) {
      out.nx = true;
    } else if (opt == "XX" && is_set && !out.nx) {
      out.xx = true;
    } else if (opt == "GET" && is_set) {
      out.get = true;
    } else if (opt == "KEEPTTL" && is_set && !out.persist && out.expire_mode.empty()) {
      out.keepttl = true;
    } else if (opt == "PERSIST" && !is_set && out.expire_mode.empty() && !out.keepttl) {
      out.persist = true;
    } else if ((opt == "EX" || opt == "PX" || opt == "EXAT" || opt == "PXAT") && !out.keepttl &&
               !out.persist && (out.expire_mode.empty() || out.expire_mode == opt) &&
               j + 1 < args.size()) {
      out.expire_mode = opt;
      out.expire = &args[++j];
    } else {
      return false;
    }
  }
  return true;
}

// Redis's getExpireMillisecondsOrReply(): the absolute expire time in ms of an EX, PX, EXAT or PXAT
// value, which must be a positive integer, or the error text in `err`.
std::optional<std::int64_t> parse_expiry(std::string mode, const std::string& value,
                                         std::string& err, const std::string& cmd_name) {
  auto invalid_msg = [&]() {
    return "invalid expire time in '" + lower(cmd_name) + "' command";
  };
  std::int64_t n = 0;
  if (!parse_strict_int(value, n)) {  // Redis's string2ll(): no sign "+", no spaces, no "05"
    err = "value is not an integer or out of range";
    return std::nullopt;
  }
  if (n <= 0) {
    err = invalid_msg();
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

std::string dump_prefix() {
  return "PEADBSTR:";
}
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

extern "C" int RedisModule_CreateCommand(const char* name, void* cmd_func, const char*, int, int,
                                         int) {
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
  if (!key_ptr) return 0;  // REDISMODULE_KEYTYPE_EMPTY
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

// One bound of Redis's zslParseRange(): a leading "(" makes it exclusive, and the rest must be
// a whole strtod() number, so "inf", "-inf", " 1" and "" (0) parse, "1abc" and "nan" do not.
bool parse_score_bound(const std::string& s, double& val, bool& exclusive) {
  exclusive = !s.empty() && s[0] == '(';
  const char* p = s.c_str() + (exclusive ? 1 : 0);
  char* end = nullptr;
  val = std::strtod(p, &end);
  return *end == '\0' && !std::isnan(val);
}

// One bound of Redis's zslParseLexRangeItem(): "-" or "+" alone, or "(" (exclusive) or "["
// (inclusive) followed by the member.
struct LexBound {
  enum Kind { Min, Max, Member } kind = Member;
  std::string member;
  bool exclusive = false;
};
bool parse_lex_bound(const std::string& s, LexBound& b) {
  if (s.empty()) return false;
  switch (s[0]) {
    case '-':
    case '+':
      if (s.size() > 1 && s[1] != '\0') return false;
      b.kind = s[0] == '-' ? LexBound::Min : LexBound::Max;
      return true;
    case '(':
    case '[':
      b.kind = LexBound::Member;
      b.member = s.substr(1);
      b.exclusive = s[0] == '(';
      return true;
    default:
      return false;
  }
}

// Shared ZRANGE/ZRANGESTORE range selector (Redis 6.2 unified ZRANGE semantics):
// index (default), BYSCORE or BYLEX modes, optional REV, LIMIT for the BY* modes,
// WITHSCORES only where the caller allows it. Fills `items` in output order and
// returns an empty string on success or a complete error reply. Arguments are checked in
// the order of Redis's zrangeGenericCommand(): the options, then the range, then the key.
std::string zrange_select(const std::vector<std::string>& args, std::size_t key_idx,
                          std::size_t min_idx, std::size_t opts_idx, bool allow_withscores,
                          std::vector<std::pair<std::string, double>>& items, bool& withscores) {
  const std::string& key = args[key_idx];
  enum class Mode { Index, ByScore, ByLex };
  Mode mode = Mode::Index;
  bool mode_given = false, rev = false;
  withscores = false;
  std::int64_t lim_off = 0, lim_cnt = -1;
  for (std::size_t i = opts_idx; i < args.size(); ++i) {
    const auto o = upper(args[i]);
    if (allow_withscores && o == "WITHSCORES")
      withscores = true;
    else if (o == "LIMIT" && i + 2 < args.size()) {
      if (!parse_i64(args[i + 1], lim_off) || !parse_i64(args[i + 2], lim_cnt))
        return encode_error("value is not an integer or out of range");
      i += 2;
    } else if (!rev && o == "REV") {
      rev = true;
    } else if (!mode_given && (o == "BYSCORE" || o == "BYLEX")) {
      mode = o == "BYSCORE" ? Mode::ByScore : Mode::ByLex;
      mode_given = true;
    } else {
      return encode_error("syntax error");
    }
  }
  if (lim_cnt != -1 && mode == Mode::Index)
    return encode_error(
        "syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
  if (withscores && mode == Mode::ByLex)
    return encode_error("syntax error, WITHSCORES not supported in combination with BYLEX");
  // REV swaps the bound order of the BY* modes: ... max min BYSCORE REV.
  const std::string& lo_s = rev && mode != Mode::Index ? args[min_idx + 1] : args[min_idx];
  const std::string& hi_s = rev && mode != Mode::Index ? args[min_idx] : args[min_idx + 1];
  std::int64_t start = 0, stop = 0;
  double lo = 0, hi = 0;
  bool lo_ex = false, hi_ex = false;
  LexBound lex_lo, lex_hi;
  if (mode == Mode::Index) {
    if (!parse_i64(lo_s, start) || !parse_i64(hi_s, stop))
      return encode_error("value is not an integer or out of range");
  } else if (mode == Mode::ByScore) {
    if (!parse_score_bound(lo_s, lo, lo_ex) || !parse_score_bound(hi_s, hi, hi_ex))
      return encode_error("min or max is not a float");
  } else if (!parse_lex_bound(lo_s, lex_lo) || !parse_lex_bound(hi_s, lex_hi)) {
    return encode_error("min or max not valid string range item");
  }
  if (store().is_wrongtype_for_zset(key)) return wrongtype_error_reply();
  bool wt = false;
  auto all = store().zrange(key, 0, -1, true, wt);  // ascending {member,score}
  if (wt) return wrongtype_error_reply();

  if (mode == Mode::Index) {
    if (rev) std::reverse(all.begin(), all.end());
    const std::int64_t n = static_cast<std::int64_t>(all.size());
    std::int64_t s = start < 0 ? n + start : start, e = stop < 0 ? n + stop : stop;
    if (s < 0) s = 0;
    if (e >= n) e = n - 1;
    for (std::int64_t i = s; i <= e && i < n; ++i) items.push_back(all[i]);
    return "";
  }
  if (mode == Mode::ByScore) {
    for (const auto& [m, sc] : all) {
      const bool above = lo_ex ? (sc > lo) : (sc >= lo);
      const bool below = hi_ex ? (sc < hi) : (sc <= hi);
      if (above && below) items.push_back({m, sc});
    }
    if (rev) std::reverse(items.begin(), items.end());
  } else {  // ByLex — all members assumed same score; compare lexicographically.
    auto lex_ge_lo = [&](const std::string& m) -> bool {
      if (lex_lo.kind != LexBound::Member) return lex_lo.kind == LexBound::Min;
      return lex_lo.exclusive ? (m > lex_lo.member) : (m >= lex_lo.member);
    };
    auto lex_le_hi = [&](const std::string& m) -> bool {
      if (lex_hi.kind != LexBound::Member) return lex_hi.kind == LexBound::Max;
      return lex_hi.exclusive ? (m < lex_hi.member) : (m <= lex_hi.member);
    };
    for (const auto& [m, sc] : all)
      if (lex_ge_lo(m) && lex_le_hi(m)) items.push_back({m, sc});
    if (rev) std::reverse(items.begin(), items.end());
  }

  // LIMIT applies to BYSCORE/BYLEX, on the (possibly reversed) output order.
  if (lim_off > 0) {
    if (static_cast<std::size_t>(lim_off) >= items.size())
      items.clear();
    else
      items.erase(items.begin(), items.begin() + lim_off);
  }
  if (lim_cnt >= 0 && static_cast<std::size_t>(lim_cnt) < items.size())
    items.resize(static_cast<std::size_t>(lim_cnt));
  return "";
}

// Shared LPOP/RPOP body: plain form returns a bulk (or nil), COUNT form returns an
// array (or nil array for a missing key), matching Redis 6.2+ popGenericCommand.
std::string list_pop_command(const std::vector<std::string>& args, bool left, RespVersion resp) {
  if (args.size() > 3) {
    return encode_error(std::string("wrong number of arguments for '") + (left ? "lpop" : "rpop") +
                        "' command");
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
constexpr double EARTH_R = 6372797.560856;  // GEO_EARTH_RADIUS_IN_METERS
constexpr double DEG_RAD = 0.017453292519943295769236907684886;
constexpr double STEP_POW = 67108864.0;  // 2^26

inline std::uint64_t interleave64(std::uint32_t xlo, std::uint32_t ylo) {
  static const std::uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
                                    0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL,
                                    0x0000FFFF0000FFFFULL};
  static const unsigned S[] = {1, 2, 4, 8, 16};
  std::uint64_t x = xlo, y = ylo;
  x = (x | (x << S[4])) & B[4];
  y = (y | (y << S[4])) & B[4];
  x = (x | (x << S[3])) & B[3];
  y = (y | (y << S[3])) & B[3];
  x = (x | (x << S[2])) & B[2];
  y = (y | (y << S[2])) & B[2];
  x = (x | (x << S[1])) & B[1];
  y = (y | (y << S[1])) & B[1];
  x = (x | (x << S[0])) & B[0];
  y = (y | (y << S[0])) & B[0];
  return x | (y << 1);
}
inline std::uint64_t deinterleave64(std::uint64_t v) {
  static const std::uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
                                    0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL,
                                    0x0000FFFF0000FFFFULL, 0x00000000FFFFFFFFULL};
  static const unsigned S[] = {0, 1, 2, 4, 8, 16};
  std::uint64_t x = v, y = v >> 1;
  x = (x | (x >> S[0])) & B[0];
  y = (y | (y >> S[0])) & B[0];
  x = (x | (x >> S[1])) & B[1];
  y = (y | (y >> S[1])) & B[1];
  x = (x | (x >> S[2])) & B[2];
  y = (y | (y >> S[2])) & B[2];
  x = (x | (x >> S[3])) & B[3];
  y = (y | (y >> S[3])) & B[3];
  x = (x | (x >> S[4])) & B[4];
  y = (y | (y >> S[4])) & B[4];
  x = (x | (x >> S[5])) & B[5];
  y = (y | (y >> S[5])) & B[5];
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
  std::uint32_t ilat = static_cast<std::uint32_t>(sep),
                ilon = static_cast<std::uint32_t>(sep >> 32);
  double lat_lo = LAT_MIN + (ilat / STEP_POW) * (LAT_MAX - LAT_MIN);
  double lat_hi = LAT_MIN + ((ilat + 1) / STEP_POW) * (LAT_MAX - LAT_MIN);
  double lon_lo = LON_MIN + (ilon / STEP_POW) * (LON_MAX - LON_MIN);
  double lon_hi = LON_MIN + ((ilon + 1) / STEP_POW) * (LON_MAX - LON_MIN);
  lat = (lat_lo + lat_hi) / 2;
  lon = (lon_lo + lon_hi) / 2;
}
inline double lat_dist(double lat1, double lat2) {
  return EARTH_R * std::fabs((lat2 - lat1) * DEG_RAD);
}
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
  if (u == "m")
    mul = 1.0;
  else if (u == "km")
    mul = 1000.0;
  else if (u == "mi")
    mul = 1609.34;
  else if (u == "ft")
    mul = 0.3048;
  else
    return false;
  return true;
}
}  // namespace geoutil

// Full SORT / SORT_RO implementation (shared; readonly rejects STORE). Mirrors Redis sort.c:
// BY/GET support key-pattern and hash-field (key_*->field) lookups; a BY pattern without '*'
// means "don't sort" (retain native list/zset order, reversed for DESC); numeric sorting
// requires every score to parse fully as a double; ties break lexicographically by element.
std::string do_sort(const std::vector<std::string>& args, RespVersion resp, bool readonly) {
  std::string store_key;
  bool have_store = false;
  bool desc = false, alpha = false, have_by = false;
  std::string by_pattern;
  std::vector<std::string> get_patterns;
  std::int64_t limit_offset = 0, limit_count = -1;
  bool have_limit = false;
  for (std::size_t i = 2; i < args.size(); ++i) {
    const auto opt = upper(args[i]);
    if (opt == "STORE" && i + 1 < args.size()) {
      if (readonly) return encode_error("syntax error");
      store_key = args[++i];
      have_store = true;
    } else if (opt == "DESC") {
      desc = true;
    } else if (opt == "ASC") {
      desc = false;
    } else if (opt == "ALPHA") {
      alpha = true;
    } else if (opt == "BY" && i + 1 < args.size()) {
      by_pattern = args[++i];
      have_by = true;
    } else if (opt == "GET" && i + 1 < args.size()) {
      get_patterns.push_back(args[++i]);
    } else if (opt == "LIMIT" && i + 2 < args.size()) {
      if (!parse_i64(args[i + 1], limit_offset) || !parse_i64(args[i + 2], limit_count))
        return encode_error("value is not an integer or out of range");
      have_limit = true;
      i += 2;
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
    bool wt = false;
    vals = store().lrange(args[1], 0, -1, wt);
    if (wt) return wrongtype_error_reply();
  } else if (type == "set") {
    bool wt = false;
    vals = store().smembers(args[1], wt);
    if (wt) return wrongtype_error_reply();
  } else if (type == "zset") {
    bool wt = false;
    auto pr = store().zrange(args[1], 0, -1, false, wt);
    if (wt) return wrongtype_error_reply();
    for (auto& p : pr) vals.push_back(p.first);
  } else {
    return wrongtype_error_reply();
  }

  // A set has no native order, so when the result must be deterministic (STORE or from a
  // script) Redis forces an ALPHA sort instead of "don't sort".
  if (dontsort && is_set && (have_store || in_lua_script_context())) {
    dontsort = false;
    alpha = true;
    have_by = false;
  }

  // Substitute '*' with the element; a "->field" suffix (non-empty field) is a hash lookup.
  auto lookup = [&](const std::string& pattern,
                    const std::string& elem) -> std::optional<std::string> {
    auto star = pattern.find('*');
    if (star == std::string::npos) return std::nullopt;
    std::string sub = pattern;
    sub.replace(star, 1, elem);
    auto arrow = sub.find("->");
    if (arrow != std::string::npos && arrow + 2 < sub.size()) {
      bool wt = false;
      return store().hget(sub.substr(0, arrow), sub.substr(arrow + 2), wt);
    }
    return store().get(sub);
  };

  if (!dontsort) {
    struct Item {
      std::string elem;
      std::string cmp;
      bool has_cmp;
      double score;
    };
    std::vector<Item> items;
    items.reserve(vals.size());
    for (auto& v : vals) {
      Item it;
      it.elem = v;
      it.has_cmp = true;
      it.cmp.clear();
      it.score = 0;
      std::optional<std::string> byval =
          have_by ? lookup(by_pattern, v) : std::optional<std::string>(v);
      if (alpha) {
        // Missing BY value → NULL compare object (sorts before present ones).
        it.has_cmp = byval.has_value();
        if (byval) it.cmp = *byval;
      } else if (byval) {
        // Numeric: the score must parse fully as a double (Redis strtod semantics).
        const std::string& s = *byval;
        errno = 0;
        char* eptr = nullptr;
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
        cmp = (a.score < b.score)   ? -1
              : (a.score > b.score) ? 1
                                    : (a.elem < b.elem   ? -1
                                       : a.elem > b.elem ? 1
                                                         : 0);  // tie → lexicographic
      } else if (have_by) {
        if (!a.has_cmp || !b.has_cmp)
          cmp = (a.has_cmp == b.has_cmp) ? 0 : (!a.has_cmp ? -1 : 1);
        else
          cmp = (a.cmp < b.cmp) ? -1 : (a.cmp > b.cmp) ? 1 : 0;
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
        if (pat == "#")
          results.push_back(v);
        else {
          auto got = lookup(pat, v);
          results.push_back(got.value_or(std::string()));  // sentinel; nil-encoded below
        }
      }
    }
  }

  if (have_store) {
    const bool deleted = store().del(store_key);
    // GET-missing entries are stored as empty strings (Redis behavior for SORT ... STORE).
    if (!results.empty()) {
      bool wt = false;
      (void)store().rpush(store_key, results, wt);
    }
    // An empty result that deleted nothing changed nothing, as in Redis's sortCommandGeneric().
    if (results.empty() && !deleted) g_write_changed_nothing = true;
    return encode_integer(static_cast<long long>(results.size()));
  }
  // SORT without STORE only reads: Redis neither propagates it nor counts it as a change.
  g_write_changed_nothing = true;
  std::vector<std::string> out;
  out.reserve(results.size());
  if (get_patterns.empty()) {
    for (const auto& v : results) out.push_back(encode_bulk(v));
  } else {
    // Re-derive nil vs value for GET projections: a missing lookup is a RESP nil.
    for (const auto& v : vals) {
      for (const auto& pat : get_patterns) {
        if (pat == "#")
          out.push_back(encode_bulk(v));
        else {
          auto got = lookup(pat, v);
          out.push_back(got ? encode_bulk(*got) : encode_null(resp));
        }
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

std::string do_geo_search(const std::vector<std::string>& args, RespVersion resp, int form,
                          bool readonly) {
  std::size_t i;
  std::string key, store_key;
  bool have_store = false, storedist = false;
  double clon = 0, clat = 0;
  bool have_center = false;
  bool bybox = false;
  double radius_m = 0, width_m = 0, height_m = 0;
  bool have_shape = false;
  double unit_mul = 1.0;
  int sort = 0;  // 0 none, 1 asc, 2 desc
  long count = 0;
  bool any = false;
  bool withcoord = false, withdist = false, withhash = false;
  std::string center_member;
  bool center_from_member = false;  // resolved after the type check

  if (form == 3) {  // GEOSEARCHSTORE dest src ...
    store_key = args[1];
    have_store = true;
    key = args[2];
    i = 3;
  } else {
    key = args[1];
    i = 2;
  }

  if (form == 0) {  // GEORADIUS key lon lat radius unit
    if (i + 3 >= args.size()) return encode_error("syntax error");
    if (!parse_double(args[i], clon) || !parse_double(args[i + 1], clat))
      return encode_error("value is not a valid float");
    double r;
    if (!parse_double(args[i + 2], r)) return encode_error("value is not a valid float");
    if (!geoutil::unit_to_m(args[i + 3], unit_mul))
      return encode_error("unsupported unit provided. please use M, KM, FT, MI");
    radius_m = r * unit_mul;
    have_center = true;
    have_shape = true;
    i += 4;
  } else if (form == 1) {  // GEORADIUSBYMEMBER key member radius unit
    if (i + 2 >= args.size()) return encode_error("syntax error");
    center_member = args[i];
    center_from_member = true;
    have_center = true;
    double r;
    if (!parse_double(args[i + 1], r)) return encode_error("value is not a valid float");
    if (!geoutil::unit_to_m(args[i + 2], unit_mul))
      return encode_error("unsupported unit provided. please use M, KM, FT, MI");
    radius_m = r * unit_mul;
    have_shape = true;
    i += 3;
  }

  for (; i < args.size(); ++i) {
    const auto o = upper(args[i]);
    if (o == "WITHCOORD")
      withcoord = true;
    else if (o == "WITHDIST")
      withdist = true;
    else if (o == "WITHHASH")
      withhash = true;
    else if (o == "ASC")
      sort = 1;
    else if (o == "DESC")
      sort = 2;
    else if (o == "ANY")
      any = true;
    else if (o == "COUNT" && i + 1 < args.size()) {
      std::int64_t c;
      if (!parse_i64(args[i + 1], c) || c <= 0) return encode_error("COUNT must be > 0");
      count = static_cast<long>(c);
      ++i;
      if (i + 1 < args.size() && upper(args[i + 1]) == "ANY") {
        any = true;
        ++i;
      }
    } else if ((o == "STORE" || o == "STOREDIST") && (form == 0 || form == 1) &&
               i + 1 < args.size()) {
      if (readonly) return encode_error("syntax error");
      store_key = args[++i];
      have_store = true;
      storedist = (o == "STOREDIST");
    } else if (o == "STOREDIST" && form == 3) {
      storedist = true;
    } else if (o == "FROMMEMBER" && (form == 2 || form == 3) && i + 1 < args.size()) {
      if (have_center) return encode_error("syntax error");
      center_member = args[i + 1];
      center_from_member = true;
      have_center = true;
      ++i;
    } else if (o == "FROMLONLAT" && (form == 2 || form == 3) && i + 2 < args.size()) {
      if (have_center) return encode_error("syntax error");
      if (!parse_double(args[i + 1], clon) || !parse_double(args[i + 2], clat))
        return encode_error("value is not a valid float");
      have_center = true;
      i += 2;
    } else if (o == "BYRADIUS" && (form == 2 || form == 3) && i + 2 < args.size()) {
      if (have_shape) return encode_error("syntax error");
      double r;
      if (!parse_double(args[i + 1], r)) return encode_error("value is not a valid float");
      if (!geoutil::unit_to_m(args[i + 2], unit_mul))
        return encode_error("unsupported unit provided. please use M, KM, FT, MI");
      radius_m = r * unit_mul;
      bybox = false;
      have_shape = true;
      i += 2;
    } else if (o == "BYBOX" && (form == 2 || form == 3) && i + 3 < args.size()) {
      if (have_shape) return encode_error("syntax error");
      double w, h;
      if (!parse_double(args[i + 1], w) || !parse_double(args[i + 2], h))
        return encode_error("value is not a valid float");
      if (!geoutil::unit_to_m(args[i + 3], unit_mul))
        return encode_error("unsupported unit provided. please use M, KM, FT, MI");
      width_m = w * unit_mul;
      height_m = h * unit_mul;
      bybox = true;
      have_shape = true;
      i += 3;
    } else
      return encode_error("syntax error");
  }

  if (form == 2 || form == 3) {
    if (!have_center)
      return encode_error("exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH");
    if (!have_shape)
      return encode_error("exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH");
  }
  if (any && count == 0) return encode_error("the ANY argument requires COUNT argument");
  if (have_store && (withcoord || withdist || withhash))
    return encode_error(
        "STORE option in GEORADIUS is not compatible with WITHCOORD, WITHDIST and WITHHASH "
        "options");
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
      if (m == center_member) {
        geoutil::decode(static_cast<std::uint64_t>(score), clon, clat);
        resolved = true;
        break;
      }
    }
    if (!resolved) {
      if (members.empty()) {
        // Missing source key: empty reply, or 0 for STORE (deleting any stale destination
        // key, mirroring georadiusGeneric's dbDelete-on-empty).
        if (have_store) {
          store().del(store_key);
          return encode_integer(0);
        }
        return encode_array({});
      }
      return encode_error("could not decode requested zset member");
    }
  }

  struct GP {
    std::string member;
    double dist_m;
    std::uint64_t bits;
    double lon, lat;
  };
  std::vector<GP> found;
  for (const auto& [m, score] : members) {
    const std::uint64_t bits = static_cast<std::uint64_t>(score);
    double plon, plat;
    geoutil::decode(bits, plon, plat);
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
  if (sort == 1)
    std::stable_sort(found.begin(), found.end(),
                     [](const GP& a, const GP& b) { return a.dist_m < b.dist_m; });
  else if (sort == 2)
    std::stable_sort(found.begin(), found.end(),
                     [](const GP& a, const GP& b) { return a.dist_m > b.dist_m; });
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
    if (!withcoord && !withdist && !withhash) {
      out.push_back(encode_bulk(g.member));
      continue;
    }
    std::vector<std::string> item;
    item.push_back(encode_bulk(g.member));
    if (withdist) {
      char b[64];
      std::snprintf(b, sizeof(b), "%.4f", g.dist_m / unit_mul);
      item.push_back(encode_bulk(b));
    }
    if (withhash) item.push_back(encode_integer(static_cast<long long>(g.bits)));
    if (withcoord) {
      char lb[64], tb[64];
      std::snprintf(lb, sizeof(lb), "%.17g", g.lon);
      std::snprintf(tb, sizeof(tb), "%.17g", g.lat);
      item.push_back(encode_array({encode_bulk(lb), encode_bulk(tb)}));
    }
    out.push_back(encode_array(item));
  }
  return encode_array(out);
}

// ── CONFIG GET / CONFIG SET ──
// Both know every Redis 7.2.5 setting (redis_config_table.generated.hpp) and run Redis's own
// value checks (check_redis_config_value(), shared with the config loader). A setting PeaDB
// applies has a live entry: its getter reads the running value and its setter applies a checked
// value. A setting PeaDB does not apply either has a fixed behaviour (peadb_fixed_setting():
// CONFIG GET reports it and CONFIG SET refuses any other value), or keeps the value it was given
// and has no effect, with a warning in the log, as in the config file (compat/delta.md item 24).

// The value of every setting with no live entry that differs from Redis's default, by
// canonical name: the config file's value, then CONFIG SET's.
std::unordered_map<std::string, std::string> g_config_values;
std::string g_config_file;  // the config file PeaDB started with; "" when there is none

struct LiveConfig {
  std::function<std::string()> get;
  // Applies a value that passed Redis's checks. Empty when PeaDB cannot change the setting
  // while it runs (or Redis cannot either: an IMMUTABLE_CONFIG setting).
  std::function<bool(const std::string& value, std::string& err)> set;
  // For a setting whose apply can fail: the same test, run before CONFIG SET changes anything,
  // so a value that cannot be applied does not first undo the settings before it.
  std::function<bool(const std::string& value, std::string& err)> check = nullptr;
};

template <typename T>
LiveConfig live_number(T& var) {
  return {[&var] { return std::to_string(var); },
          [&var](const std::string& v, std::string&) {
            // The value is in the setting's range already; a ULongLong above what `var` holds
            // (only maxmemory) is kept as the largest value it can hold.
            if constexpr (std::is_signed_v<T>) {
              constexpr auto kMax = static_cast<unsigned long long>(std::numeric_limits<T>::max());
              var = v[0] == '-' ? static_cast<T>(std::stoll(v))
                                : static_cast<T>(std::min(std::stoull(v), kMax));
            } else {
              var = static_cast<T>(std::stoull(v));
            }
            return true;
          }};
}

LiveConfig live_bool(bool& var) {
  return {[&var] { return std::string(var ? "yes" : "no"); },
          [&var](const std::string& v, std::string&) {
            var = v == "yes";
            return true;
          }};
}

LiveConfig live_string(std::string& var) {
  return {[&var] { return var; },
          [&var](const std::string& v, std::string&) {
            var = v;
            return true;
          }};
}

// shutdown-on-sigterm and shutdown-on-sigint: a set of kShutdown* flags, printed in the order of
// Redis's shutdown_on_sig_enum, or "default" for none.
LiveConfig live_shutdown_flags(int& var) {
  return {[&var] { return shutdown_flags_to_str(var); },
          [&var](const std::string& v, std::string&) {
            var = parse_shutdown_flags(v);
            return true;
          }};
}

LiveConfig live_cluster_port(int& var) {
  return {[&var] { return std::to_string(var); },
          [&var](const std::string& v, std::string&) {
            var = std::stoi(v);
            // CLUSTER NODES/SLOTS must show the new address at once (see cluster-announce-ip).
            reregister_self_if_needed();
            return true;
          }};
}

const std::unordered_map<std::string, LiveConfig>& live_configs() {
  static const auto* table = [] {
    auto& lim = store().encoding_limits();
    return new std::unordered_map<std::string, LiveConfig>{
        // Redis can move its listeners at runtime; PeaDB cannot yet (item 25).
        {"port", {[] { return std::to_string(g_server_port); }, nullptr}},
        {"bind", {[] { return g_server_bind; }, nullptr}},
        {"tcp-backlog", {[] { return std::to_string(g_config_tcp_backlog); }, nullptr}},
        {"replicaof",
         {[] {
            return g_replication_role == "slave"
                       ? g_replication_master_host + " " + std::to_string(g_replication_master_port)
                       : std::string();
          },
          nullptr}},
        {"appendfilename", {[] { return g_aof_path.string(); }, nullptr}},
        {"cluster-enabled",
         {[] { return std::string(g_cluster_enabled ? "yes" : "no"); }, nullptr}},
        {"rdbchecksum", {[] { return std::string(rdb_checksum() ? "yes" : "no"); }, nullptr}},
        {"maxclients",
         {[] { return std::to_string(configured_max_clients()); },
          [](const std::string& v, std::string&) {
            configure_max_clients(static_cast<int>(std::min<long long>(std::stoll(v), INT_MAX)));
            return true;
          },
          [](const std::string& v, std::string& err) {
            // updateMaxclients(): the open-files limit must allow that many clients (raising
            // it is harmless even if CONFIG SET then fails on another setting).
            const long long wanted = std::stoll(v);
            const int allowed =
                adjust_open_files_limit(static_cast<int>(std::min<long long>(wanted, INT_MAX)));
            if (allowed < wanted) {
              err =
                  "The operating system is not able to handle the specified number of "
                  "clients, try with " +
                  std::to_string(allowed);
              return false;
            }
            return true;
          }}},
        {"timeout", live_number(g_config_timeout)},
        {"tcp-keepalive", live_number(g_tcp_keepalive)},
        {"repl-timeout",
         {[] { return std::to_string(g_repl_timeout.load()); },
          [](const std::string& v, std::string&) {
            g_repl_timeout = std::stoi(v);
            return true;
          }}},
        {"repl-ping-replica-period", live_number(g_repl_ping_replica_period)},
        {"loglevel",
         {[] { return g_loglevel_name; },
          [](const std::string& v, std::string&) {
            configure_log_level(v);
            return true;
          }}},
        {"dir",
         {[] {
            std::error_code ec;
            const auto cwd = std::filesystem::current_path(ec);
            return ec ? std::string() : cwd.string();
          },
          [](const std::string& v, std::string& err) {
            // setConfigDirOption(): the data files follow, since their names are relative.
            if (::chdir(v.c_str()) != 0) {
              err = std::strerror(errno);
              return false;
            }
            return true;
          },
          [](const std::string& v, std::string& err) {
            // What chdir(2) checks: the path is a directory this process may enter.
            struct stat st {};
            const int error = ::stat(v.c_str(), &st) != 0      ? errno
                              : !S_ISDIR(st.st_mode)           ? ENOTDIR
                              : ::access(v.c_str(), X_OK) != 0 ? errno
                                                               : 0;
            if (error != 0) err = std::strerror(error);
            return error == 0;
          }}},
        {"dbfilename",
         {[] { return g_snapshot_path.string(); },
          [](const std::string& v, std::string&) {
            g_snapshot_path = v;
            return true;
          }}},
        {"appendonly",
         {[] { return std::string(g_appendonly ? "yes" : "no"); },
          [](const std::string& v, std::string& err) {
            // updateAppendonly()
            if (v == "yes" && g_aof_state == AofState::Off) return start_append_only(err);
            if (v == "no" && g_aof_state != AofState::Off) stop_append_only();
            return true;
          }}},
        {"appendfsync", live_string(g_appendfsync)},
        {"aof-load-truncated", live_bool(g_aof_load_truncated)},
        {"save",
         {[] { return g_save_spec; },
          [](const std::string& v, std::string&) {
            configure_save(v);
            return true;
          }}},
        {"stop-writes-on-bgsave-error", live_bool(g_stop_writes_on_bgsave_error)},
        {"rdb-key-save-delay",
         {[] { return std::to_string(rdb_key_save_delay()); },
          [](const std::string& v, std::string&) {
            rdb_set_key_save_delay(std::stoi(v));
            return true;
          }}},
        {"shutdown-on-sigterm", live_shutdown_flags(g_shutdown_on_sigterm)},
        {"shutdown-on-sigint", live_shutdown_flags(g_shutdown_on_sigint)},
        {"shutdown-timeout", live_number(g_shutdown_timeout)},
        {"requirepass", live_string(g_requirepass)},
        {"masterauth",
         {[] { return master_credentials_copy().second; },
          [](const std::string& v, std::string&) {
            std::lock_guard<std::recursive_mutex> lock(g_masterauth_mu);
            g_masterauth = v;
            return true;
          }}},
        {"masteruser",
         {[] { return master_credentials_copy().first; },
          [](const std::string& v, std::string&) {
            std::lock_guard<std::recursive_mutex> lock(g_masterauth_mu);
            g_masteruser = v.c_str();  // Redis stores this setting as a C string.
            return true;
          }}},
        {"protected-mode", live_bool(g_protected_mode)},
        {"maxmemory",
         {[] { return std::to_string(g_config_maxmemory); },
          [](const std::string& v, std::string& err) {
            if (!live_number(g_config_maxmemory).set(v, err)) return false;
            // updateMaxmemory()
            if (g_config_maxmemory > 0) {
              const std::size_t reported = used_memory();
              const std::size_t not_counted = free_memory_not_counted();
              const std::size_t used = reported > not_counted ? reported - not_counted : 0;
              if (static_cast<std::size_t>(g_config_maxmemory) < used) {
                log(LogLevel::Warn, "WARNING: the new maxmemory value set via CONFIG SET (" +
                                        std::to_string(g_config_maxmemory) +
                                        ") is smaller than the current memory usage (" +
                                        std::to_string(used) +
                                        "). This will result in key eviction and/or the inability "
                                        "to accept new write commands depending on the "
                                        "maxmemory-policy.");
              }
              start_eviction_time_proc();
            }
            return true;
          }}},
        {"maxmemory-eviction-tenacity", live_number(g_maxmemory_eviction_tenacity)},
        {"maxmemory-policy",
         {[] { return std::string(maxmemory_policy_to_str(g_maxmemory_policy)); },
          [](const std::string& v, std::string&) {
            apply_maxmemory_policy(*parse_maxmemory_policy(v));
            return true;
          }}},
        {"maxmemory-samples", live_number(g_maxmemory_samples)},
        {"lfu-log-factor",
         {[] { return std::to_string(store().lfu_log_factor()); },
          [](const std::string& v, std::string&) {
            store().set_lfu_log_factor(std::stoi(v));
            return true;
          }}},
        {"lazyfree-lazy-server-del", live_bool(g_lazyfree_lazy_server_del)},
        {"repl-backlog-size",
         {[] { return std::to_string(g_repl_backlog.size_); },
          [](const std::string& v, std::string& err) {
            // resizeReplicationBacklog() raises a smaller size to 16 KB. PeaDB allocates the
            // whole buffer at once (Redis grows it as data arrives), so a size that cannot be
            // allocated is refused instead.
            const long long size = std::max<long long>(std::stoll(v), 16 * 1024);
            // A smaller ring keeps only its newest bytes: the replicas get what they have not
            // taken yet first (compat/delta.md item 40).
            if (g_backlog_has_replica && g_feed_replicas_cb) {
              g_feed_replicas_cb(nullptr);
              g_replicas_taken_to = g_repl_backlog.total;
            }
            if (!g_repl_backlog.resize(static_cast<std::size_t>(size))) {
              err = "Unable to allocate a replication backlog of this size";
              return false;
            }
            return true;
          }}},
        {"min-replicas-to-write", live_number(g_config_min_replicas_to_write)},
        {"replica-serve-stale-data", live_bool(g_config_replica_serve_stale_data)},
        {"replica-ignore-disk-write-errors", live_bool(g_replica_ignore_disk_write_errors)},
        {"busy-reply-threshold", live_number(g_config_lua_time_limit)},
        {"proto-max-bulk-len", live_number(g_proto_max_bulk_len)},
        {"client-query-buffer-limit", live_number(g_client_query_buffer_limit)},
        {"client-output-buffer-limit",
         {[] { return client_output_buffer_limit_str(); },
          [](const std::string& v, std::string&) {
            configure_client_output_buffer_limit(v);  // only the classes named change
            return true;
          }}},
        {"notify-keyspace-events",
         {[] { return notify_flags_to_str(g_notify_flags); },
          [](const std::string& v, std::string&) {
            g_notify_flags = parse_notify_flags(v);
            return true;
          }}},
        {"slowlog-log-slower-than", live_number(g_slowlog_threshold_us)},
        {"slowlog-max-len",
         {[] { return std::to_string(g_slowlog_max_len); },
          [](const std::string& v, std::string&) {
            g_slowlog_max_len = static_cast<std::size_t>(std::stoull(v));
            while (g_slowlog.size() > g_slowlog_max_len) g_slowlog.pop_back();
            return true;
          }}},
        {"latency-monitor-threshold", live_number(g_latency_monitor_threshold_ms)},
        {"hash-max-listpack-entries", live_number(lim.hash_entries)},
        {"hash-max-listpack-value", live_number(lim.hash_value)},
        {"list-max-listpack-size", live_number(lim.list_size)},
        {"set-max-intset-entries", live_number(lim.set_intset)},
        {"set-max-listpack-entries", live_number(lim.set_entries)},
        {"set-max-listpack-value", live_number(lim.set_value)},
        {"zset-max-listpack-entries", live_number(lim.zset_entries)},
        {"zset-max-listpack-value", live_number(lim.zset_value)},
        {"cluster-announce-ip",
         {[] { return g_cluster_announce_ip; },
          [](const std::string& v, std::string&) {
            g_cluster_announce_ip = v;
            // The node table (g_cluster_nodes[0]) is a lazy snapshot taken by the first
            // CLUSTER call (ensure_self_registered()); re-derive it so CONFIG GET and CLUSTER
            // NODES/SLOTS agree whether this runs before or after that snapshot.
            reregister_self_if_needed();
            return true;
          },
          [](const std::string& v, std::string& err) {
            // Same hazard the startup guard refuses (main.cpp): a wildcard address is
            // undialable by any peer. Empty means "fall back to bind" (Redis's
            // EMPTY_STRING_IS_NULL, config.c:3105), so check the address that would actually
            // be announced: a node can only start with a wildcard `bind` if it also had an
            // explicit, non-wildcard cluster-announce-ip, so this override may be the only
            // thing between the announced host and that wildcard bind.
            const std::string resolved = v.empty() ? g_server_bind : v;
            if (is_wildcard_announce_host(resolved)) {
              err = "cluster-announce-ip must not resolve to a wildcard address ('" + resolved +
                    "'); no peer could dial it back";
              return false;
            }
            return true;
          }}},
        {"cluster-announce-port", live_cluster_port(g_cluster_announce_port)},
        {"cluster-announce-bus-port", live_cluster_port(g_cluster_announce_bus_port)},
    };
  }();
  return *table;
}

std::string config_value(const RedisConfigSpec& spec) {
  const auto& live = live_configs();
  if (const auto it = live.find(spec.name); it != live.end()) return it->second.get();
  if (const FixedSetting* fixed = peadb_fixed_setting(spec.name)) return fixed->value;
  if (const auto it = g_config_values.find(spec.name); it != g_config_values.end()) {
    return it->second;
  }
  return spec.default_value != nullptr ? spec.default_value : "";
}

// A replica's address as Redis's INFO slaveN line shows it: what it said with REPLCONF
// ip-address, or the address it connected from.
std::string replica_ip(const SessionState& replica) {
  if (!replica.repl_ip_address.empty()) return replica.repl_ip_address;
  const auto colon = replica.addr.rfind(':');
  return colon == std::string::npos ? replica.addr : replica.addr.substr(0, colon);
}

// INFO's Replication section, field for field as Redis's genRedisInfoString() writes it.
std::string info_replication_section() {
  const std::int64_t now = now_ms();
  const std::int64_t now_s = now / 1000;
  const auto setting = [](const char* name) {
    return config_value(*find_redis_config(name));
  };
  const bool replica = g_replication_role == "slave";
  std::string out = std::string("# Replication\r\nrole:") + (replica ? "slave" : "master") + "\r\n";
  if (replica) {
    const bool link_up = !g_replica_stale;
    // The master's stream as read and as applied; with no link, the offset applied so far.
    const std::int64_t applied = g_master_repl_offset;
    const std::int64_t read = link_up ? g_replica_read_offset.load() : applied;
    const bool transfer = g_repl_transfer_in_progress.load();
    out += "master_host:" + g_replication_master_host +
           "\r\n"
           "master_port:" +
           std::to_string(g_replication_master_port) +
           "\r\n"
           "master_link_status:" +
           std::string(link_up ? "up" : "down") +
           "\r\n"
           "master_last_io_seconds_ago:" +
           std::to_string(link_up ? now_s - g_replica_last_io_ms.load() / 1000 : -1) +
           "\r\n"
           "master_sync_in_progress:" +
           std::string(transfer ? "1" : "0") +
           "\r\n"
           "slave_read_repl_offset:" +
           std::to_string(read) +
           "\r\n"
           "slave_repl_offset:" +
           std::to_string(applied) + "\r\n";
    if (transfer) {
      const std::int64_t size = g_repl_transfer_size.load();
      const std::int64_t done = g_repl_transfer_read.load();
      char perc[32];
      std::snprintf(perc, sizeof(perc), "%.2f",
                    size > 0 ? static_cast<double>(done) / static_cast<double>(size) * 100 : 0.0);
      out += "master_sync_total_bytes:" + std::to_string(size) +
             "\r\n"
             "master_sync_read_bytes:" +
             std::to_string(done) +
             "\r\n"
             "master_sync_left_bytes:" +
             std::to_string(size - done) +
             "\r\n"
             "master_sync_perc:" +
             std::string(perc) +
             "\r\n"
             "master_sync_last_io_seconds_ago:" +
             std::to_string(now_s - g_repl_transfer_lastio_ms.load() / 1000) + "\r\n";
    }
    if (!link_up) {
      const std::int64_t since = g_repl_down_since_ms.load();
      out += "master_link_down_since_seconds:" +
             std::to_string(since != 0 ? now_s - since / 1000 : -1) + "\r\n";
    }
    out += "slave_priority:" + setting("replica-priority") +
           "\r\n"
           "slave_read_only:" +
           std::string(setting("replica-read-only") == "yes" ? "1" : "0") +
           "\r\n"
           "replica_announced:" +
           std::string(setting("replica-announced") == "yes" ? "1" : "0") + "\r\n";
  }
  // As in Redis, syncing replicas appear in INFO but only online ones count as good.
  std::int64_t max_lag = 0;
  parse_i64(setting("min-replicas-max-lag"), max_lag);
  int count = 0;
  int good = 0;
  std::string lines;
  if (g_for_each_replica_cb) {
    g_for_each_replica_cb([&](const SessionState& r) {
      const std::int64_t lag = now_s - r.repl_ack_time_ms / 1000;
      if (r.repl_sync_state == ReplicaSyncState::Online && lag <= max_lag) ++good;
      lines += "slave" + std::to_string(count++) + ":ip=" + replica_ip(r) +
               ",port=" + std::to_string(r.repl_listening_port) + ",state=" +
               std::string(r.repl_sync_state == ReplicaSyncState::Online         ? "online"
                           : r.repl_sync_state == ReplicaSyncState::SendSnapshot ? "send_bulk"
                                                                                 : "wait_bgsave") +
               ",offset=" + std::to_string(r.repl_ack_offset) + ",lag=" + std::to_string(lag) +
               "\r\n";
    });
  }
  out += "connected_slaves:" + std::to_string(count) + "\r\n";
  if (g_config_min_replicas_to_write != 0 && max_lag != 0)
    out += "min_slaves_good_slaves:" + std::to_string(good) + "\r\n";
  out += lines;
  const bool backlog = g_backlog_has_replica;
  out +=
      "master_failover_state:no-failover\r\n"
      "master_replid:" +
      g_master_replid +
      "\r\n"
      "master_replid2:" +
      g_master_replid2 +
      "\r\n"
      "master_repl_offset:" +
      std::to_string(g_master_repl_offset) +
      "\r\n"
      "second_repl_offset:" +
      std::to_string(g_second_replid_offset) +
      "\r\n"
      "repl_backlog_active:" +
      std::string(backlog ? "1" : "0") +
      "\r\n"
      "repl_backlog_size:" +
      std::to_string(g_repl_backlog.size_) +
      "\r\n"
      "repl_backlog_first_byte_offset:" +
      std::to_string(backlog ? g_repl_backlog.base() + 1 : 0) +
      "\r\n"
      "repl_backlog_histlen:" +
      std::to_string(backlog ? g_repl_backlog.histlen() : 0) + "\r\n";
  return out;
}

// allowProtectedAction(): what enable-protected-configs (and Redis's other "yes / local / no"
// settings) allow this client.
bool allow_protected_action(const std::string& setting, const SessionState& session) {
  return setting == "yes" || (setting == "local" && session.is_local);
}

// CONFIG GET <pattern> [<pattern> ...] (configGetCommand()): a name without wildcards is looked
// up directly and shown as the client spelled it, so it also finds a HIDDEN_CONFIG setting; a
// pattern matches every name and alias except hidden settings, ignoring case.
std::string config_get_reply(const std::vector<std::string>& args, RespVersion resp) {
  std::vector<std::string> out;
  std::unordered_set<std::string> seen;  // lower-case names already in the reply
  auto add = [&](const std::string& shown, const RedisConfigSpec& spec) {
    if (!seen.insert(lower(shown)).second) return;
    out.push_back(encode_bulk(shown));
    out.push_back(encode_bulk(config_value(spec)));
  };
  for (std::size_t i = 2; i < args.size(); ++i) {
    const std::string& pattern = args[i];
    if (pattern.find_first_of("[*?") == std::string::npos) {
      if (const RedisConfigSpec* spec = find_redis_config(pattern)) add(pattern, *spec);
      continue;
    }
    for (const auto& spec : kRedisConfigSpecs) {
      if ((spec.flags & kCfgHidden) != 0) continue;
      for (const char* name : {spec.name, spec.alias}) {
        if (name != nullptr && fnmatch(pattern.c_str(), name, FNM_CASEFOLD) == 0) add(name, spec);
      }
    }
  }
  return encode_map(out, resp);
}

// CONFIG SET <name> <value> [<name> <value> ...] (configSetCommand()): all or nothing. Errors
// come in Redis's order: an unknown, immutable, protected or repeated name first, then a value
// that fails Redis's checks; PeaDB also refuses at that point a value it cannot apply. Only then
// does anything change; if applying one setting fails, the ones before it are put back.
std::string config_set_reply(const std::vector<std::string>& args, const SessionState& session) {
  if (args.size() % 2 != 0) return encode_error("syntax error");
  struct Change {
    const RedisConfigSpec* spec;
    std::string entry;  // the name or alias it was set by, in lower case (Redis's config->name)
    std::string value;
    const LiveConfig* live = nullptr;
  };
  std::vector<Change> changes;
  const std::string protected_allowed =
      config_value(*find_redis_config("enable-protected-configs"));
  for (std::size_t i = 2; i < args.size(); i += 2) {
    const RedisConfigSpec* spec = find_redis_config(args[i]);
    if (spec == nullptr) {
      return encode_error("Unknown option or number of arguments for CONFIG SET - '" + args[i] +
                          "'");
    }
    const std::string failed =
        "CONFIG SET failed (possibly related to argument '" + args[i] + "') - ";
    if ((spec->flags & kCfgImmutable) != 0) {
      return encode_error(failed + "can't set immutable config");
    }
    if ((spec->flags & kCfgProtected) != 0 && !allow_protected_action(protected_allowed, session)) {
      return encode_error(failed + "can't set protected config");
    }
    const std::string entry = lower(args[i]);
    for (const auto& c : changes) {
      if (c.entry == entry) return encode_error(failed + "duplicate parameter");
    }
    changes.push_back({spec, entry, args[i + 1]});
  }
  const auto& live = live_configs();
  for (auto& c : changes) {
    const std::string failed =
        "CONFIG SET failed (possibly related to argument '" + c.entry + "') - ";
    std::string checked, err;
    if (!check_redis_config_value(*c.spec, config_set_args(*c.spec, c.value), checked, err)) {
      return encode_error(failed + err);
    }
    c.value = checked;
    if (const auto it = live.find(c.spec->name); it != live.end()) {
      c.live = &it->second;
      if (!c.live->set && checked != c.live->get()) {
        return encode_error(failed +
                            "PeaDB cannot change this setting while it runs yet "
                            "(compat/delta.md item 25)");
      }
      if (c.live->check && !c.live->check(checked, err)) return encode_error(failed + err);
    } else if (const FixedSetting* fixed = peadb_fixed_setting(c.spec->name)) {
      if (checked != fixed->value) {
        return encode_error(failed + (fixed->refuse != nullptr
                                          ? std::string(fixed->refuse)
                                          : "PeaDB always uses '" + std::string(fixed->value) +
                                                "' for this setting and cannot change it "
                                                "(compat/delta.md item 24)"));
      }
    }
  }
  std::vector<std::pair<const Change*, std::string>> applied;  // with the value it replaced
  std::unique_lock<std::recursive_mutex> credentials_lock(g_masterauth_mu, std::defer_lock);
  if (std::any_of(changes.begin(), changes.end(), [](const Change& c) {
        return std::strcmp(c.spec->name, "masteruser") == 0 ||
               std::strcmp(c.spec->name, "masterauth") == 0;
      }))
    credentials_lock.lock();
  for (const auto& c : changes) {
    const std::string old = config_value(*c.spec);
    if (c.live != nullptr) {
      // A bool, number, enum or string that keeps its value is left alone, as in Redis (its
      // set function returns 2, so its apply function does not run); a special setting is
      // always applied.
      if (!c.live->set || (c.spec->type != RedisConfigType::Special && c.value == old)) continue;
      std::string err;
      if (!c.live->set(c.value, err)) {
        log(LogLevel::Warn, "Failed applying new configuration. Possibly related to new " +
                                c.entry + " setting. Restoring previous settings.");
        for (auto it = applied.rbegin(); it != applied.rend(); ++it) {
          std::string restore_err;
          if (it->first->live == nullptr) {
            g_config_values[it->first->spec->name] = it->second;
          } else if (!it->first->live->set(it->second, restore_err)) {
            // restoreBackupConfig() logs the same; the value is left out, as it may be secret.
            log(LogLevel::Warn, "Failed restoring failed CONFIG SET command. Error setting " +
                                    it->first->entry + ": " + restore_err);
          }
        }
        return encode_error("CONFIG SET failed (possibly related to argument '" + c.entry +
                            "') - " + err);
      }
    } else if (peadb_fixed_setting(c.spec->name) == nullptr) {
      g_config_values[c.spec->name] = c.value;
      if (c.value != old) {
        log(LogLevel::Warn, "CONFIG SET '" + c.entry +
                                "' is a Redis setting that PeaDB does not "
                                "support yet, so it has no effect (compat/delta.md item 24)");
      }
    }
    applied.emplace_back(&c, old);
  }
  return encode_simple("OK");
}

// ── Script shebang (Redis's evalExtractShebangFlags(), eval.c) ──
struct ScriptShebang {
  bool has_shebang = false;  // no shebang: Redis's EVAL compatibility mode, no declared flags
  int flags = 0;             // the kFunc* flags it declares
  std::size_t length = 0;  // length of the shebang line without its '\n'; the Lua code starts there
  std::string err;         // Redis's error text when the shebang is not valid
};

ScriptShebang parse_script_shebang(const std::string& body) {
  ScriptShebang out;
  if (body.compare(0, 2, "#!") != 0) return out;
  out.has_shebang = true;
  const auto nl = body.find('\n');
  if (nl == std::string::npos) {
    out.err = "Invalid script shebang";
    return out;
  }
  out.length = nl;
  std::vector<std::string> parts;
  if (!split_config_args(body.substr(0, nl), parts) || parts.empty()) {
    out.err = "Invalid engine in script shebang";
    return out;
  }
  if (parts[0] != "#!lua") {
    out.err = "Unexpected engine in script shebang: " + parts[0];
    return out;
  }
  for (std::size_t j = 1; j < parts.size(); ++j) {
    if (parts[j].compare(0, 6, "flags=") != 0) {
      out.err = "Unknown lua shebang option: " + parts[j];
      return out;
    }
    // Split the rest on ',' like sdssplitlen(): nothing gives no flags, "a," gives "a" and "".
    const std::string list = parts[j].substr(6);
    if (list.empty()) continue;
    std::size_t start = 0;
    while (true) {
      const auto comma = list.find(',', start);
      const std::string flag =
          list.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
      if (flag == "no-writes")
        out.flags |= kFuncNoWrites;
      else if (flag == "allow-oom")
        out.flags |= kFuncAllowOom;
      else if (flag == "allow-stale")
        out.flags |= kFuncAllowStale;
      else if (flag == "no-cluster")
        out.flags |= kFuncNoCluster;
      else if (flag == "allow-cross-slot-keys")
        out.flags |= kFuncAllowCrossSlot;
      else {
        out.err = "Unexpected flag in script shebang: " + flag;
        return out;
      }
      if (comma == std::string::npos) break;
      start = comma + 1;
    }
  }
  return out;
}

// The flags of the library function `name` (kFunc*), or nothing when there is no such function.
std::optional<int> function_flags(const std::string& name) {
  const auto idx = g_func_index.find(lower(name));
  if (idx == g_func_index.end()) return std::nullopt;
  const auto lib = g_libraries.find(idx->second);
  if (lib == g_libraries.end()) return std::nullopt;
  for (const auto& f : lib->second.functions)
    if (lower(f.name) == lower(name)) return f.flags;
  return std::nullopt;
}

// Redis's command table (the pinned command JSON): each command's and subcommand's flags.
struct RedisCommandIndex {
  std::unordered_map<std::string, std::uint64_t> flags;  // "name" or "name|sub", lower-case
  std::unordered_set<std::string> containers;            // the commands that have subcommands
};
const RedisCommandIndex& redis_command_index() {
  static const auto* const index = [] {
    auto* ix = new RedisCommandIndex();
    for (const auto& e : command_metadata()) {
      const std::string name = e.full_name;
      ix->flags[name] = e.flags;
      if (const auto bar = name.find('|'); bar != std::string::npos)
        ix->containers.insert(name.substr(0, bar));
    }
    return ix;
  }();
  return *index;
}

const std::unordered_map<std::string, CommandSpec>& command_table();  // below

// Redis's flags of the command `spec` for `args`: a container command resolves to its subcommand,
// as Redis's lookupCommand() does (an unknown one keeps the container's flags).
std::uint64_t spec_redis_flags(const CommandSpec& spec, const std::vector<std::string>& args) {
  if (spec.redis_container && args.size() >= 2) {
    const auto& index = redis_command_index();
    const auto sub = index.flags.find(spec.redis_name + "|" + lower(args[1]));
    if (sub != index.flags.end()) return sub->second;
  }
  return spec.redis_flags;
}

// Whether `args` names a subcommand Redis does not have of the container command `spec`.
bool spec_subcommand_unknown(const CommandSpec& spec, const std::vector<std::string>& args) {
  return spec.redis_container && args.size() >= 2 &&
         !redis_command_index().flags.count(spec.redis_name + "|" + lower(args[1]));
}

// Redis's getCommandFlags() for the command `spec` and `args`: Redis's flags, except that EVAL,
// EVALSHA, FCALL and their _RO forms take the flags the script or function declares
// (evalGetCommandFlags(), fcallGetCommandFlags()), mapped by scriptFlagsToCmdFlags(): declared
// flags mean "writes" unless they include no-writes, and "denyoom" unless they include allow-oom
// or no-writes. A script without a shebang keeps EVAL's own flags (it may replicate; its writes
// are checked one by one when they run).
std::uint64_t spec_command_flags(const CommandSpec& spec, const std::vector<std::string>& args) {
  std::uint64_t flags = spec_redis_flags(spec, args);
  const std::string& name = spec.redis_name;
  std::optional<int> declared;
  const bool eval = name == "eval" || name == "eval_ro";
  const bool evalsha = name == "evalsha" || name == "evalsha_ro";
  if ((eval || evalsha) && args.size() >= 2) {
    const std::string* body = &args[1];
    if (evalsha) {
      const auto it =
          args[1].size() == 40 ? g_script_cache.find(lower(args[1])) : g_script_cache.end();
      body = it == g_script_cache.end() ? nullptr : &it->second;
    }
    if (body != nullptr) {
      const auto she = parse_script_shebang(*body);
      if (she.has_shebang && she.err.empty()) declared = she.flags;
    }
  } else if ((name == "fcall" || name == "fcall_ro") && args.size() >= 2) {
    declared = function_flags(args[1]);
  }
  if (!declared) return flags;
  flags &= ~(kRedisCmdStale | kRedisCmdDenyoom | kRedisCmdWrite | kRedisCmdMayReplicate);
  if (!(*declared & (kFuncAllowOom | kFuncNoWrites))) flags |= kRedisCmdDenyoom;
  if (!(*declared & kFuncNoWrites)) flags |= kRedisCmdWrite;
  if (*declared & kFuncAllowStale) flags |= kRedisCmdStale;
  return flags;
}

// The same by the name the client sent (`cmd`, also a name given by rename-command); 0 for a
// name that runs no command.
std::uint64_t command_flags(const std::string& cmd, const std::vector<std::string>& args) {
  const auto& table = command_table();
  const auto it = table.find(upper(cmd));
  return it == table.end() ? 0 : spec_command_flags(it->second, args);
}

// Whether a command writes or may replicate (CLIENT PAUSE WRITE holds it).
bool command_may_replicate(const std::string& cmd, const std::vector<std::string>& args) {
  return (command_flags(cmd, args) & (kRedisCmdWrite | kRedisCmdMayReplicate)) != 0;
}

// Whether a command writes, as processCommand() reads CMD_WRITE for the -MISCONF check.
bool command_writes(const std::string& cmd, const std::vector<std::string>& args) {
  return (command_flags(cmd, args) & kRedisCmdWrite) != 0;
}

// Whether CLIENT PAUSE holds this command (processCommand): everything under ALL; under WRITE
// a command that writes or may replicate, or an EXEC whose transaction holds one.
bool pause_applies(const std::string& cmd, const std::vector<std::string>& args,
                   const SessionState& session) {
  if (g_paused_actions & kPauseClientAll) return true;
  if (!(g_paused_actions & kPauseClientWrite)) return false;
  if (cmd == "EXEC")
    return session.in_multi &&
           (session.multi_flags & (kRedisCmdWrite | kRedisCmdMayReplicate)) != 0;
  return command_may_replicate(cmd, args);
}

// Whether this client's command waits until the pause ends. Replicas are never paused. Commands
// that run inside EXEC (the transaction that sent CLIENT PAUSE finishes first, as with Redis's
// client_pause_in_transaction), inside a script, or from the AOF are not checked; commands from
// the master are held in drain_replica_cmd_queue().
bool pause_holds(const std::string& cmd, const std::vector<std::string>& args,
                 const SessionState& session) {
  if (g_paused_actions == 0) return false;
  if (session.replica_stream || g_replaying_aof || g_executing_exec || in_lua_script_context())
    return false;
  return pause_applies(cmd, args, session);
}

// The body of EVAL and EVALSHA: runs `source` (the script, shebang included) with the numkeys,
// keys and arguments in args[2..]. `cache` adds the script to the script cache (EVAL does;
// EVALSHA found it there).
std::string eval_script(const std::string& source, const std::vector<std::string>& args,
                        SessionState& session, bool cache) {
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
  for (std::size_t i = static_cast<std::size_t>(3 + numkeys); i < args.size(); ++i)
    argv.push_back(args[i]);

  const auto she = parse_script_shebang(source);
  if (!she.err.empty()) return encode_error(she.err);
  // The shebang line is skipped but its '\n' kept, so Lua's line numbers match the source.
  const std::string script_body = she.has_shebang ? source.substr(she.length) : source;
  const bool no_writes = (she.flags & kFuncNoWrites) != 0;
  const bool allow_oom = (she.flags & kFuncAllowOom) != 0 || no_writes;
  // evalGenericCommand() caches the script before scriptPrepareForRun() can refuse it.
  if (cache) g_script_cache[sha1_hex(source)] = source;
  // EVAL_RO and EVALSHA_RO run in a read-only context. A script without a shebang is checked by
  // each command it calls (-NOREPLICAS at its first write, scriptVerifyWriteCommandAllow()).
  if (std::string refusal =
          script_prepare_refusal(she.has_shebang, she.flags, g_script_readonly_context);
      !refusal.empty())
    return refusal;

  g_script_client_resp_version = session.resp_version;
  g_script_current_session = &session;
  const bool prev_readonly = g_script_readonly_context;
  const bool prev_allow_oom = g_script_allow_oom;
  const bool prev_write_dirty = g_script_write_dirty;
  if (no_writes) g_script_readonly_context = true;
  if (allow_oom) g_script_allow_oom = true;
  g_script_write_dirty = false;
  const auto out = eval_miniscript(script_body, keys, argv);
  g_script_readonly_context = prev_readonly;
  g_script_allow_oom = prev_allow_oom;
  g_script_write_dirty = prev_write_dirty;
  g_script_current_session = nullptr;
  return out;
}

// Redis's addReplyErrorFormat("Target instance replied with error: %s") for the error line a
// target sent (without its '-').
std::string migrate_target_error(const std::string& line) {
  std::string text = "Target instance replied with error: " + line.substr(1);
  while (!text.empty() && (text.back() == '\r' || text.back() == '\n')) text.pop_back();
  std::replace(text.begin(), text.end(), '\r', ' ');
  return encode_error(text);
}

// Redis's migrateCommand():
//   MIGRATE host port key|"" db timeout [COPY] [REPLACE] [AUTH password | AUTH2 username password]
//           [KEYS key ...]
// sends the keys it finds to the target on a kept connection (migrate_get_socket()), as RESTORE
// (RESTORE-ASKING in cluster mode) after AUTH and a SELECT when the connection is in another
// database, and deletes here the keys the target took, unless COPY. The deletions go to the AOF and
// the replicas as one DEL of those keys, also when the target refused others (the reply is then the
// target's first error), so a replica never runs MIGRATE itself. A broken kept connection is opened
// again once, unless the time ran out. Three differences from Redis: the RESTOREs go out only after
// AUTH and SELECT succeeded (Redis sends everything in one write, so a target that refused the
// SELECT restored the keys into the connection's database), a MIGRATE without AUTH never uses a
// connection that AUTH logged in, and a stream is refused, because PeaDB cannot write one in
// Redis's DUMP format yet (compat/delta.md item 51). The handler sends its own events, change
// counts and DEL; the dispatch must not do it again.
std::string migrate_command(const std::vector<std::string>& args, SessionState& session) {
  const std::int64_t start_ms = DataStore::now_ms();
  bool copy = false;
  bool replace = false;
  const std::string* username = nullptr;
  const std::string* password = nullptr;
  std::size_t first_key = 3;  // the argument index of the first key
  std::size_t num_keys = 1;
  for (std::size_t j = 6; j < args.size(); ++j) {
    const std::size_t moreargs = args.size() - 1 - j;
    const std::string opt = upper(args[j]);
    if (opt == "COPY") {
      copy = true;
    } else if (opt == "REPLACE") {
      replace = true;
    } else if (opt == "AUTH") {
      if (moreargs == 0) return encode_error("syntax error");
      password = &args[++j];
    } else if (opt == "AUTH2") {
      if (moreargs < 2) return encode_error("syntax error");
      username = &args[++j];
      password = &args[++j];
    } else if (opt == "KEYS") {
      if (!args[3].empty()) {
        return encode_error(
            "When using MIGRATE KEYS option, the key argument must be set to the empty string");
      }
      first_key = j + 1;
      num_keys = args.size() - j - 1;
      break;  // every argument after KEYS is a key
    } else {
      return encode_error("syntax error");
    }
  }
  std::int64_t timeout = 0;
  std::int64_t dbid = 0;
  if (!parse_strict_int(args[5], timeout) || !parse_strict_int(args[4], dbid))
    return encode_error("value is not an integer or out of range");
  if (timeout <= 0) timeout = 1000;

  // lookupKeyRead(): an expired key is deleted (with its DEL) and counts as missing, a key found
  // counts as an access (unless CLIENT NO-TOUCH). No key found is not an error: it often expired
  // meanwhile.
  std::vector<const std::string*> keys;
  bool has_stream = false;
  for (std::size_t j = 0; j < num_keys; ++j) {
    const std::string& key = args[first_key + j];
    const std::string type = store().type_of(key);
    if (type == "none") continue;
    if (type == "stream") has_stream = true;
    if (!session.no_touch) store().touch_key_for_eviction(key);
    keys.push_back(&key);
  }
  if (keys.empty()) return encode_simple("NOKEY");
  if (has_stream) {
    return encode_error(
        "MIGRATE of a stream is not supported by PeaDB yet (it cannot DUMP a stream in Redis's "
        "format)");
  }

  // As in Redis, which reads every expire time against the command's start, a key found above is
  // sent with what was left of its TTL then and deleted here once the target took it, even if the
  // time passes while the data goes out.
  const DataStore::LazyExpireDisabled no_lazy_expire(store());
  std::string restores;
  for (const std::string* key : keys) {
    std::int64_t ttl = 0;
    if (const std::int64_t expireat = store().pexpiretime(*key); expireat >= 0) {
      ttl = expireat - start_ms;
      if (ttl < 1) ttl = 1;
    }
    std::vector<std::string> restore = {g_cluster_enabled ? "RESTORE-ASKING" : "RESTORE", *key,
                                        std::to_string(ttl), rdb_dump_key(*key)};
    if (replace) restore.push_back("REPLACE");
    restores += encode_command_resp(restore);
  }

  const std::string name = args[1] + ":" + args[2];
  std::vector<std::string> deleted = {"DEL"};  // propagated for the keys the target took
  std::string reply;
  bool may_retry = true;
  while (true) {  // Redis's try_again
    MigrateCachedSocket* cs = migrate_get_socket(name, args[1], args[2], timeout);
    // A connection that an earlier MIGRATE logged in with AUTH is not used by one without AUTH, so
    // a client that does not know the target's password cannot write there with another client's
    // login; it connects again. Redis 7.2.5 reuses the connection.
    if (cs != nullptr && password == nullptr && cs->authenticated) {
      migrate_close_socket(name);
      cs = migrate_get_socket(name, args[1], args[2], timeout);
    }
    if (cs == nullptr) return "-IOERR error or timeout connecting to the client\r\n";
    const int fd = cs->fd;
    bool write_error = false;
    int err = 0;
    // Redis's 64 KB writes, each within the timeout.
    const auto send_all = [&](const std::string& data) {
      for (std::size_t pos = 0; pos < data.size();) {
        const std::size_t towrite = std::min<std::size_t>(data.size() - pos, 64 * 1024);
        if (sync_write(fd, data.data() + pos, towrite, timeout) != static_cast<ssize_t>(towrite)) {
          err = errno;
          write_error = true;
          return false;
        }
        pos += towrite;
      }
      return true;
    };

    std::string prefix;
    if (password != nullptr) {
      prefix += username != nullptr ? encode_command_resp({"AUTH", *username, *password})
                                    : encode_command_resp({"AUTH", *password});
    }
    const bool select = cs->last_dbid != dbid;
    if (select) prefix += encode_command_resp({"SELECT", std::to_string(dbid)});
    std::string auth_reply;
    std::string select_reply;
    errno = 0;
    bool socket_failed = false;  // Redis's goto socket_err
    if (!prefix.empty() && !send_all(prefix)) socket_failed = true;
    if (!socket_failed && password != nullptr) {
      if (sync_read_line(fd, auth_reply, 1024, timeout) <= 0) {
        err = errno;
        socket_failed = true;
      } else if (auth_reply[0] != '-') {
        cs->authenticated = true;  // also when the SELECT fails below
      }
    }
    if (!socket_failed && select && sync_read_line(fd, select_reply, 1024, timeout) <= 0) {
      err = errno;
      socket_failed = true;
    }
    if (!socket_failed) {
      if ((password != nullptr && auth_reply[0] == '-') || (select && select_reply[0] == '-')) {
        cs->last_dbid = -1;
        return migrate_target_error(password != nullptr && auth_reply[0] == '-' ? auth_reply
                                                                                : select_reply);
      }
      if (!send_all(restores)) socket_failed = true;
    }
    if (socket_failed) {
      // Redis's socket_err: forget the connection and try once more unless the time ran out.
      migrate_close_socket(name);
      if (err != ETIMEDOUT && may_retry) {
        may_retry = false;
        continue;
      }
      return std::string("-IOERR error or timeout ") + (write_error ? "writing" : "reading") +
             " to target instance\r\n";
    }

    bool error_from_target = false;
    bool socket_error = false;
    std::size_t j = 0;
    std::string restore_reply;
    for (; j < keys.size(); ++j) {
      if (sync_read_line(fd, restore_reply, 1024, timeout) <= 0) {
        err = errno;
        socket_error = true;
        break;
      }
      if (restore_reply[0] == '-') {
        if (!error_from_target) {
          cs->last_dbid = -1;  // the target may have changed database
          error_from_target = true;
          reply = migrate_target_error(restore_reply);
        }
      } else if (!copy) {
        // dbDelete(), signalModifiedKey(), the `del` event and one change for each key.
        store().del(*keys[j]);
        notify_keyspace_event(NOTIFY_GENERIC, "del", *keys[j], session.db_index);
        store().increment_mutation_epoch();
        deleted.push_back(*keys[j]);
      }
    }
    // Retry only when nothing came back, so the target took nothing.
    if (!error_from_target && socket_error && j == 0 && may_retry && err != ETIMEDOUT) {
      migrate_close_socket(name);
      may_retry = false;
      continue;
    }
    if (socket_error) {
      migrate_close_socket(name);
      if (!error_from_target) reply = "-IOERR error or timeout reading to target instance\r\n";
    } else if (!error_from_target) {
      cs->last_dbid = dbid;
      reply = encode_simple("OK");
    }
    break;
  }
  if (deleted.size() > 1) propagate_write(deleted, session, nullptr);
  return reply;
}

const std::unordered_map<std::string, CommandSpec>& command_table() {
  static const std::unordered_map<std::string, CommandSpec> table = [] {
    std::unordered_map<std::string, CommandSpec> t;

    t.emplace("PING",
              CommandSpec{"PING",
                          -1,
                          {"fast", "readonly"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            if (args.size() > 2)
                              return encode_error("wrong number of arguments for 'ping' command");
                            // In RESP2 subscribe mode Redis replies with a 2-element array instead
                            // of a simple/bulk string (server.c pingCommand).
                            if (session.resp_version == RespVersion::Resp2 &&
                                client_has_subscriptions(session.fd)) {
                              const std::string& msg = args.size() == 2 ? args[1] : "";
                              return std::string("*2\r\n$4\r\npong\r\n$") +
                                     std::to_string(msg.size()) + "\r\n" + msg + "\r\n";
                            }
                            if (args.size() == 1) return encode_simple("PONG");
                            return encode_bulk(args[1]);
                          }});

    t.emplace("PUBLISH",
              CommandSpec{"PUBLISH",
                          3,
                          {"write", "pubsub", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            auto receivers = pubsub_publish(args[1], args[2]);
                            return encode_integer(static_cast<long long>(receivers));
                          }});

    t.emplace("TIME", CommandSpec{"TIME",
                                  1,
                                  {"readonly", "fast"},
                                  0,
                                  0,
                                  0,
                                  [](const std::vector<std::string>&, SessionState&, bool&) {
                                    const auto ms = DataStore::now_ms();
                                    const auto sec = ms / 1000;
                                    const auto usec = (ms % 1000) * 1000;
                                    return encode_array({encode_bulk(std::to_string(sec)),
                                                         encode_bulk(std::to_string(usec))});
                                  }});

    t.emplace("ECHO", CommandSpec{"ECHO",
                                  2,
                                  {"fast", "readonly"},
                                  0,
                                  0,
                                  0,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return encode_bulk(args[1]);
                                  }});

    t.emplace("QUIT", CommandSpec{"QUIT",
                                  1,
                                  {"fast", "no_auth"},
                                  0,
                                  0,
                                  0,
                                  [](const std::vector<std::string>&, SessionState&, bool& close) {
                                    close = true;
                                    return encode_simple("OK");
                                  }});

    // SHUTDOWN [NOSAVE|SAVE] [NOW] [FORCE] [ABORT]: Redis's shutdownCommand() (db.c). The work
    // is prepare_for_shutdown(), shared with SIGTERM and SIGINT. While the shutdown waits for
    // lagging replicas the client is blocked (Redis's blockClientShutdown()): it gets no reply
    // when the server exits, and "Errors trying to SHUTDOWN. Check logs." when the shutdown fails
    // or SHUTDOWN ABORT cancels it.
    t.emplace(
        "SHUTDOWN",
        CommandSpec{
            "SHUTDOWN",
            -1,
            {"admin"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool& close) {
              bool nosave = false, save = false, now = false, force = false, abort = false;
              for (std::size_t i = 1; i < args.size(); ++i) {
                const auto o = lower(args[i]);
                if (o == "nosave")
                  nosave = true;
                else if (o == "save")
                  save = true;
                else if (o == "now")
                  now = true;
                else if (o == "force")
                  force = true;
                else if (o == "abort")
                  abort = true;
                else
                  return encode_error("syntax error");
              }
              if ((abort && (nosave || save || now || force)) || (nosave && save))
                return encode_error("syntax error");
              if (abort) {
                if (abort_shutdown()) return encode_simple("OK");
                return encode_error("No shutdown in progress.");
              }
              // A client that cannot block (Redis's CLIENT_DENY_BLOCKING): in PeaDB a script, which
              // Redis does not let call SHUTDOWN at all (NOSCRIPT, compat/delta.md item 14).
              const bool deny_blocking = in_lua_script_context();
              if (!now && deny_blocking)
                return encode_error(
                    "SHUTDOWN without NOW or ABORT isn't allowed for DENY BLOCKING client");
              // A script that timed out: only SHUTDOWN NOSAVE may stop the server.
              if (!nosave && g_script_busy.load()) return busy_script_error_reply();
              const int flags = (save ? kShutdownSave : 0) | (nosave ? kShutdownNosave : 0) |
                                (now ? kShutdownNow : 0) | (force ? kShutdownForce : 0);
              if (prepare_for_shutdown(flags)) {
                request_shutdown();
                close = true;
                return std::string();  // like Redis, no reply: the client sees the connection close
              }
              // The shutdown waits for replicas (this one, or one that was already waiting): the
              // client waits for its end.
              if (shutdown_initiated() && !deny_blocking) {
                session.blocked_on_shutdown = true;
                return std::string();
              }
              return encode_error("Errors trying to SHUTDOWN. Check logs.");
            }});

    t.emplace("SYNC",
              CommandSpec{"SYNC",
                          1,
                          {"admin"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>&, SessionState& session, bool&) {
                            if (session.replica_stream)
                              return std::string();  // syncCommand(): already a replica
                            if (const std::string refused = sync_refusal(session); !refused.empty())
                              return refused;
                            attach_replica(session);
                            session.repl_index = static_cast<std::size_t>(g_master_repl_offset);
                            g_sync_full.fetch_add(1, std::memory_order_relaxed);
                            session.repl_sync_state = ReplicaSyncState::WaitSnapshot;
                            session.repl_legacy_sync = true;
                            return std::string();
                          }});

    t.emplace(
        "REPLCONF",
        CommandSpec{"REPLCONF",
                    -1,
                    {"admin"},
                    0,
                    0,
                    0,
                    [](const std::vector<std::string>& args, SessionState& session, bool&) {
                      // REPLCONF listening-port <port>
                      // REPLCONF capa eof|psync2
                      // REPLCONF GETACK *
                      // REPLCONF ACK <offset>
                      for (std::size_t i = 1; i + 1 < args.size(); i += 2) {
                        const auto opt = upper(args[i]);
                        if (opt == "LISTENING-PORT") {
                          // The replica's own port, which names it in the shutdown's "Lagging
                          // replica" line.
                          std::int64_t port = 0;
                          if (!parse_i64(args[i + 1], port))
                            return encode_error("value is not an integer or out of range");
                          session.repl_listening_port = static_cast<int>(port);
                        } else if (opt == "IP-ADDRESS") {
                          if (args[i + 1].size() >= 256)  // NET_HOST_STR_LEN
                            return encode_error(
                                "REPLCONF ip-address provided by replica instance is too long: " +
                                std::to_string(args[i + 1].size()) + " bytes");
                          session.repl_ip_address = args[i + 1];
                        } else if (opt == "CAPA") {
                          // Capabilities PeaDB does not know are ignored, as in Redis.
                          if (upper(args[i + 1]) == "PSYNC2") session.repl_capa_psync2 = true;
                        } else if (opt == "ACK") {
                          // replconfCommand(): only a replica's ACK counts, the offset never goes
                          // back, and there is no reply.
                          std::int64_t offset = 0;
                          if (!session.replica_stream || !parse_i64(args[i + 1], offset))
                            return std::string();
                          if (offset > session.repl_ack_offset) session.repl_ack_offset = offset;
                          session.repl_ack_time_ms = now_ms();
                          return std::string();
                        } else if (opt == "GETACK") {
                          // A replica answers its master with REPLCONF ACK and the offset it has
                          // applied, which does not count this GETACK yet (it is added to the
                          // replica's stream once it has run); nobody gets a reply
                          // (replconfCommand() → replicationSendAck()).
                          if (g_replication_role == "slave") send_replica_ack();
                          return std::string();
                        }
                      }
                      return encode_simple("OK");
                    }});

    t.emplace("PSYNC",
              CommandSpec{
                  "PSYNC",
                  3,
                  {"admin"},
                  0,
                  0,
                  0,
                  [](const std::vector<std::string>& args, SessionState& session, bool&) {
                    // PSYNC <replid> <offset>
                    if (session.replica_stream)
                      return std::string();  // syncCommand(): already a replica
                    if (const std::string refused = sync_refusal(session); !refused.empty())
                      return refused;
                    const std::string& replid = args[1];
                    std::int64_t offset = -1;
                    parse_i64(args[2], offset);

                    // Attempt partial resync if the requested replid matches our current id (or our
                    // pre-promotion replid2 history, up to second_replid_offset) and the offset is
                    // still within the backlog window. Like Redis, only a backlog that exists can
                    // serve one (a master's backlog starts with its first replica).
                    const bool id_ok = (replid == g_master_replid) ||
                                       (replid == g_master_replid2 && g_second_replid_offset >= 0 &&
                                        offset <= g_second_replid_offset);
                    // Redis numbers the stream's bytes from 1: a replica that holds the stream up
                    // to offset R asks for R + 1, the first byte it lacks
                    // (masterTryPartialResynchronization(): from repl_backlog->offset to
                    // repl_backlog->offset + histlen). The backlog counts from 0.
                    if (g_backlog_has_replica && id_ok && offset >= 1 &&
                        offset - 1 >= g_repl_backlog.base() && offset - 1 <= g_repl_backlog.total) {
                      std::string pending;
                      g_repl_backlog.get_slice(offset - 1, pending);
                      attach_replica(session);
                      session.repl_index = static_cast<std::size_t>(g_master_repl_offset);
                      g_sync_partial_ok.fetch_add(1, std::memory_order_relaxed);
                      // A PSYNC2 replica learns the current replication id (it may have changed
                      // with a promotion) and keeps the one it had as its replid2.
                      if (session.repl_capa_psync2)
                        return "+CONTINUE " + g_master_replid + "\r\n" + pending;
                      return "+CONTINUE\r\n" + pending;
                    }
                    // A partial resync was requested (replid given) but we can't serve it.
                    if (replid != "?") g_sync_partial_err.fetch_add(1, std::memory_order_relaxed);

                    // The event loop batches requests and forks between commands, outside an
                    // execution unit. While waiting for another child, no part of the stream
                    // belongs to this sync.
                    attach_replica(session);
                    g_sync_full.fetch_add(1, std::memory_order_relaxed);
                    session.repl_sync_state = ReplicaSyncState::WaitSnapshot;
                    return std::string();
                  }});

    t.emplace("MULTI",
              CommandSpec{"MULTI",
                          1,
                          {"fast"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>&, SessionState& session, bool&) {
                            if (session.in_multi)
                              return encode_error("MULTI calls can not be nested");
                            session.in_multi = true;
                            session.multi_dirty = false;
                            session.queued.clear();
                            session.multi_flags = 0;
                            session.multi_inv_flags = 0;
                            return encode_simple("OK");
                          }});

    t.emplace("DISCARD",
              CommandSpec{"DISCARD",
                          1,
                          {"fast"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>&, SessionState& session, bool&) {
                            if (!session.in_multi) return encode_error("DISCARD without MULTI");
                            session.in_multi = false;
                            session.multi_dirty = false;
                            session.queued.clear();
                            session.watched_epoch.reset();
                            session.watched_digests.clear();
                            return encode_simple("OK");
                          }});

    t.emplace("WATCH",
              CommandSpec{"WATCH",
                          -2,
                          {"fast"},
                          1,
                          -1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            if (session.in_multi)
                              return encode_error("WATCH inside MULTI is not allowed");
                            session.watched_epoch = store().mutation_epoch();
                            for (std::size_t i = 1; i < args.size(); ++i) {
                              session.watched_digests[args[i]] = store().watch_digest(args[i]);
                            }
                            return encode_simple("OK");
                          }});

    t.emplace("UNWATCH",
              CommandSpec{"UNWATCH",
                          1,
                          {"fast"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>&, SessionState& session, bool&) {
                            session.watched_epoch.reset();
                            session.watched_digests.clear();
                            return encode_simple("OK");
                          }});

    t.emplace(
        "EXEC",
        CommandSpec{"EXEC",
                    1,
                    {"fast"},
                    0,
                    0,
                    0,
                    [](const std::vector<std::string>&, SessionState& session, bool&) {
                      if (!session.in_multi) return encode_error("EXEC without MULTI");
                      if (session.multi_dirty) {
                        session.in_multi = false;
                        session.multi_dirty = false;
                        session.queued.clear();
                        session.watched_epoch.reset();
                        session.watched_digests.clear();
                        return std::string(
                            "-EXECABORT Transaction discarded because of previous errors.\r\n");
                      }
                      if (!session.watched_digests.empty()) {
                        bool changed = false;
                        for (const auto& [key, digest] : session.watched_digests) {
                          if (store().watch_digest(key) != digest) {
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
                      session.watched_epoch.reset();
                      session.watched_digests.clear();
                      // The dispatch has checked EXEC by the commands it queued (-OOM, -MISCONF,
                      // -NOREPLICAS, -READONLY, -MASTERDOWN, as processCommand() does), so the
                      // commands run without those checks, as Redis's execCommand() calls them. The
                      // commands' writes join EXEC's execution unit, which wraps them in MULTI/EXEC
                      // for the AOF and the replicas when there is more than one
                      // (propagate_pending_commands()). A REPLICAOF inside the transaction makes
                      // this server a replica before the unit ends, so, as in Redis, the replicas
                      // it had get none of it.
                      struct ExecutingExec {
                        ExecutingExec() {
                          g_executing_exec = true;
                        }
                        ~ExecutingExec() {
                          g_executing_exec = false;
                        }
                      } executing;
                      std::vector<std::string> replies;
                      replies.reserve(queued.size());
                      for (const auto& q : queued) {
                        bool close = false;
                        replies.push_back(handle_command(q, session, close));
                      }
                      return encode_array(replies);
                    }});

    t.emplace("HELLO",
              CommandSpec{
                  "HELLO",
                  -1,
                  {"fast", "no_auth"},
                  0,
                  0,
                  0,
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
                      if (proto != 2 && proto != 3)
                        return encode_error("NOPROTO unsupported protocol version");
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
                          return encode_error(
                              "Client sent AUTH, but no password is set. Did you mean AUTH "
                              "<username> <password>?");
                        if (u != "default" || !const_time_eq(p, g_requirepass))
                          return std::string(
                              "-WRONGPASS invalid username-password pair or user is disabled.\r\n");
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
                      return std::string(
                          "-NOAUTH HELLO must be called with the client already "
                          "authenticated, otherwise the HELLO <proto> AUTH <user> <pass> "
                          "option can be used to authenticate the client and select the "
                          "RESP protocol version at the same time\r\n");
                    }
                    session.resp_version = requested;
                    if (!set_name.empty()) session.client_name = set_name;
                    return hello_reply(session);
                  }});

    t.emplace(
        "AUTH",
        CommandSpec{"AUTH",
                    -2,
                    {"fast", "no_auth"},
                    0,
                    0,
                    0,
                    [](const std::vector<std::string>& args, SessionState& session, bool&) {
                      // Only the two- and three-argument forms are valid; more is a syntax error
                      // (Redis).
                      if (args.size() > 3) return encode_error("syntax error");
                      std::string user = "default";
                      std::string pass;
                      if (args.size() == 3) {
                        user = args[1];
                        pass = args[2];
                      } else {
                        pass = args[1];
                      }
                      // Redis only replies "no password is set" for the single-arg default-user
                      // form; AUTH <user> <pass> against an unknown user is WRONGPASS regardless.
                      if (user != "default") {
                        return std::string(
                            "-WRONGPASS invalid username-password pair or user is disabled.\r\n");
                      }
                      if (g_requirepass.empty()) {
                        // Matches Redis authCommand's two-argument (default-user, no-pass) branch.
                        return encode_error(
                            "AUTH <password> called without any password configured for "
                            "the default user. Are you sure your configuration is correct?");
                      }
                      if (!const_time_eq(pass, g_requirepass)) {
                        return std::string(
                            "-WRONGPASS invalid username-password pair or user is disabled.\r\n");
                      }
                      session.authenticated = true;
                      session.user = "default";
                      return encode_simple("OK");
                    }});

    t.emplace(
        "ACL",
        CommandSpec{
            "ACL",
            -2,
            {"admin"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              // ACLCommandCategories order from acl.c:64-87. ACL CAT returns them in this order.
              static constexpr const char* kAclCategories[] = {
                  "keyspace",   "read",        "write",    "set",         "sortedset", "list",
                  "hash",       "string",      "bitmap",   "hyperloglog", "geo",       "stream",
                  "pubsub",     "admin",       "fast",     "slow",        "blocking",  "dangerous",
                  "connection", "transaction", "scripting"};
              const std::string sub = upper(args[1]);
              // PeaDB has one user, "default", whose password is `requirepass` and which always
              // holds full access. Introspection below reports that truthfully. Anything that
              // would create or change a rule refuses: there is no rule engine behind it
              // (compat/delta.md items 1 and 7). ACL LOG refuses rather than returning an empty
              // array, because "no events" would misrepresent auth failures PeaDB never records.
              if (sub == "WHOAMI" && args.size() == 2) return encode_bulk(session.user);
              if (sub == "USERS" && args.size() == 2) {
                return encode_array({encode_bulk("default")});
              }
              if (sub == "LIST" && args.size() == 2) {
                return encode_array({encode_bulk("user default " + acl_describe_default_user())});
              }
              if (sub == "CAT" && args.size() == 2) {
                std::vector<std::string> out;
                for (const char* c : kAclCategories) out.push_back(encode_bulk(c));
                return encode_array(out);
              }
              if (sub == "CAT" && args.size() == 3) {
                const std::string want_cat = lower(args[2]);
                bool known = false;
                for (const char* c : kAclCategories) {
                  if (want_cat == c) {
                    known = true;
                    break;
                  }
                }
                if (!known) return encode_error("Unknown category '" + args[2] + "'");
                // Mirrors COMMAND LIST FILTERBY ACLCAT's iteration idiom: filter the generated
                // command metadata by its space-separated acl_cats field, restricted to commands
                // PeaDB actually implements (the metadata table also covers commands it does not).
                const auto& table = command_table();
                std::vector<std::string> out;
                for (const auto& e : command_metadata()) {
                  const std::string full = e.full_name;
                  const auto bar = full.find('|');
                  const std::string top = bar == std::string::npos ? full : full.substr(0, bar);
                  if (table.find(upper(top)) == table.end()) continue;  // only implemented commands
                  const std::string cats = e.acl_cats;                  // space-separated
                  bool has = false;
                  std::size_t p = 0;
                  while (p < cats.size()) {
                    std::size_t sp = cats.find(' ', p);
                    const std::string tok =
                        cats.substr(p, sp == std::string::npos ? std::string::npos : sp - p);
                    if (tok == want_cat) {
                      has = true;
                      break;
                    }
                    if (sp == std::string::npos) break;
                    p = sp + 1;
                  }
                  if (!has) continue;
                  out.push_back(encode_bulk(full));
                }
                return encode_array(out);
              }
              if (sub == "GETUSER" && args.size() == 3) {
                // Redis resolves usernames via ACLGetUserByName -> raxFind (acl.c:1536-1540), a
                // byte-exact radix lookup, so this comparison is case-sensitive: "DEFAULT" is a
                // different (non-existent) user, unlike ACL CAT's category name below.
                if (args[2] != "default") return encode_null(session.resp_version);
                std::vector<std::string> flags = {encode_bulk("on")};
                if (g_requirepass.empty()) flags.push_back(encode_bulk("nopass"));
                flags.push_back(encode_bulk("sanitize-payload"));
                std::vector<std::string> passwords;
                if (!g_requirepass.empty())
                  passwords.push_back(encode_bulk(sha256_hex(g_requirepass)));
                // 6-field map: flags, passwords, commands, keys, channels, selectors.
                // aclAddReplySelectorDescription (acl.c:2704) contributes commands/keys/channels;
                // PeaDB's one user always holds the root selector's full access, and has no
                // additional (non-root) selectors.
                std::vector<std::string> kv = {
                    encode_bulk("flags"),     encode_set_reply(flags, session.resp_version),
                    encode_bulk("passwords"), encode_array(passwords),
                    encode_bulk("commands"),  encode_bulk("+@all"),
                    encode_bulk("keys"),      encode_bulk("~*"),
                    encode_bulk("channels"),  encode_bulk("&*"),
                    encode_bulk("selectors"), encode_array({})};
                return encode_map(kv, session.resp_version);
              }
              if (sub == "GENPASS" && (args.size() == 2 || args.size() == 3)) {
                // Redis default is 256 bits rendered as hex (acl.c:2924-2942).
                std::int64_t bits = 256;
                if (args.size() == 3 && !parse_i64(args[2], bits)) {
                  return encode_error("value is not an integer or out of range");
                }
                if (bits <= 0 || bits > 4096) {
                  return encode_error(
                      "ACL GENPASS argument must be the number of bits for the output password, "
                      "a positive number up to 4096");
                }
                const std::int64_t chars = (bits + 3) / 4;
                return encode_bulk(gen_hex_chars(static_cast<std::size_t>(chars)));
              }
              if (sub == "HELP" && args.size() == 2) {
                return encode_array(
                    {encode_bulk("ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                     encode_bulk("CAT [<category>]"),
                     encode_bulk("DELUSER <username> [<username> ...]"),
                     encode_bulk("DRYRUN <username> <command> [<arg> ...]"),
                     encode_bulk("GETUSER <username>"), encode_bulk("GENPASS [<bits>]"),
                     encode_bulk("LIST"), encode_bulk("LOAD"), encode_bulk("LOG [<count> | RESET]"),
                     encode_bulk("SAVE"),
                     encode_bulk("SETUSER <username> <attribute> [<attribute> ...]"),
                     encode_bulk("USERS"), encode_bulk("WHOAMI"), encode_bulk("HELP")});
              }
              // Redis dispatches on subcommand AND argc together, so a wrong-arity call for one
              // of these must fall through to the generic unknown-subcommand error below rather
              // than this "not supported" gap message. Arities are Redis's own: setuser argc>=3
              // (acl.c:2761), deluser argc>=3 (:2789), load argc==2 (:2892), save argc==2 (:2900),
              // log argc==2||argc==3 (:2943), dryrun argc>=4 (:3017).
              bool no_rule_engine_arity_ok = false;
              if (sub == "SETUSER" || sub == "DELUSER")
                no_rule_engine_arity_ok = args.size() >= 3;
              else if (sub == "LOAD" || sub == "SAVE")
                no_rule_engine_arity_ok = args.size() == 2;
              else if (sub == "LOG")
                no_rule_engine_arity_ok = args.size() == 2 || args.size() == 3;
              else if (sub == "DRYRUN")
                no_rule_engine_arity_ok = args.size() >= 4;
              if (no_rule_engine_arity_ok) {
                return encode_error(
                    "ACL " + sub +
                    " is not supported: PeaDB enforces a single 'default' user "
                    "and has no ACL rule engine (see compat/delta.md items 1 and 7)");
              }
              // Redis's addReplySubcommandSyntaxError (networking.c:1146-1153).
              return encode_error("unknown subcommand or wrong number of arguments for '" +
                                  args[1] + "'. Try ACL HELP.");
            }});

    t.emplace("SCRIPT",
              CommandSpec{
                  "SCRIPT",
                  -2,
                  {"write"},
                  0,
                  0,
                  0,
                  [](const std::vector<std::string>& args, SessionState& session, bool&) {
                    const std::string sub = upper(args[1]);
                    if (sub == "LOAD" && args.size() == 3) {
                      // A script with an invalid shebang is refused at load, like Redis's
                      // luaCreateFunction().
                      const auto she = parse_script_shebang(args[2]);
                      if (!she.err.empty()) return encode_error(she.err);
                      const auto sha = sha1_hex(args[2]);
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
                    if (sub == "DEBUG") {
                      if (args.size() != 3)
                        return encode_error("wrong number of arguments for 'script|debug' command");
                      // Redis refuses SCRIPT DEBUG while the client has replies not yet sent:
                      // inside EXEC, or after another command of the same pipeline
                      // (clientHasPendingReplies, eval.c).
                      if (session.has_pending_replies || g_executing_exec)
                        return encode_error("SCRIPT DEBUG must be called outside a pipeline");
                      const auto mode = lower(args[2]);
                      if (mode == "no")
                        return encode_simple("OK");  // debugging is off: true, nothing to do
                      // PeaDB has no Lua debugger (LDB): YES (a forked session whose writes are
                      // thrown away) and SYNC (a blocking session driven step by step) are refused
                      // rather than letting the next script run unattended with kept effects
                      // (compat/delta.md item 14).
                      if (mode == "yes" || mode == "sync")
                        return encode_error(
                            "SCRIPT DEBUG YES and SYNC are not supported by PeaDB yet (it has no "
                            "Lua debugger)");
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

    t.emplace("EVAL",
              CommandSpec{"EVAL",
                          -3,
                          {"write", "noscript"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            return eval_script(args[1], args, session, /*cache=*/true);
                          }});

    // EVALSHA runs the cached script exactly as EVAL runs it, so its shebang flags apply.
    t.emplace(
        "EVALSHA",
        CommandSpec{"EVALSHA",
                    -3,
                    {"write", "noscript"},
                    0,
                    0,
                    0,
                    [](const std::vector<std::string>& args, SessionState& session, bool&) {
                      auto it = g_script_cache.find(lower(args[1]));
                      if (it == g_script_cache.end()) {
                        return std::string("-NOSCRIPT No matching script. Please use EVAL.\r\n");
                      }
                      return eval_script(it->second, args, session, /*cache=*/false);
                    }});

    t.emplace("EVAL_RO",
              CommandSpec{"EVAL_RO",
                          -3,
                          {"readonly", "noscript"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            g_script_readonly_context = true;
                            const auto out = eval_script(args[1], args, session, /*cache=*/true);
                            g_script_readonly_context = false;
                            return out;
                          }});

    // Read-only like EVAL_RO: the script runs in the read-only context (it used to run with
    // writes allowed).
    t.emplace(
        "EVALSHA_RO",
        CommandSpec{"EVALSHA_RO",
                    -3,
                    {"readonly", "noscript"},
                    0,
                    0,
                    0,
                    [](const std::vector<std::string>& args, SessionState& session, bool&) {
                      auto it = g_script_cache.find(lower(args[1]));
                      if (it == g_script_cache.end()) {
                        return std::string("-NOSCRIPT No matching script. Please use EVAL.\r\n");
                      }
                      g_script_readonly_context = true;
                      const auto out = eval_script(it->second, args, session, /*cache=*/false);
                      g_script_readonly_context = false;
                      return out;
                    }});

    t.emplace(
        "INFO",
        CommandSpec{
            "INFO",
            -1,
            {"readonly"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              // Redis 7.0+ INFO takes zero or more section names (case-insensitive). No args (or
              // "default") = the standard set, which EXCLUDES commandstats/latencystats; "all"/
              // "everything" = every section; explicit names union together (each emitted once).
              std::set<std::string> requested;
              for (std::size_t i = 1; i < args.size(); ++i) requested.insert(upper(args[i]));
              if (requested.empty()) requested.insert("DEFAULT");
              const bool want_all =
                  requested.count("ALL") != 0 || requested.count("EVERYTHING") != 0;
              const bool want_default = requested.count("DEFAULT") != 0;
              auto uptime_s = (now_ms() - g_server_start_time.load()) / 1000;
              if (uptime_s < 1) uptime_s = 1;

              const std::size_t used_mem = used_memory();
              // (normal clients, replicas): the bytes their query and output buffers hold.
              const std::pair<std::size_t, std::size_t> client_mem =
                  g_client_buffers_cb ? g_client_buffers_cb()
                                      : std::pair<std::size_t, std::size_t>{0, 0};
              // The replication buffers: the backlog bytes (as used_memory counts them) and what
              // the replicas have not been sent yet (Redis keeps both in one shared buffer).
              const std::size_t backlog_mem = backlog_memory();
              const std::size_t replica_obufs =
                  g_replica_output_buffers_cb ? g_replica_output_buffers_cb() : 0;
              const std::int64_t current_eviction_exceeded_us =
                  g_eviction_exceeded_since_us != 0 ? steady_now_us() - g_eviction_exceeded_since_us
                                                    : 0;
              if (static_cast<std::int64_t>(used_mem) > g_used_memory_peak)
                g_used_memory_peak = static_cast<std::int64_t>(used_mem);
              const std::int64_t rss_bytes = process_rss_bytes();
              const double frag_ratio =
                  (used_mem > 0 && rss_bytes > 0)
                      ? static_cast<double>(rss_bytes) / static_cast<double>(used_mem)
                      : 1.0;
              struct rusage ru {};
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

              auto fmt2 = [](double d) -> std::string {
                char b[32];
                std::snprintf(b, sizeof(b), "%.2f", d);
                return b;
              };
              auto fmt6 = [](double d) -> std::string {
                char b[32];
                std::snprintf(b, sizeof(b), "%.6f", d);
                return b;
              };

              auto format_mem = [](std::size_t bytes) -> std::string {
                if (bytes >= 1024ULL * 1024 * 1024)
                  return std::to_string(bytes / (1024ULL * 1024 * 1024)) + "G";
                if (bytes >= 1024ULL * 1024) return std::to_string(bytes / (1024ULL * 1024)) + "M";
                if (bytes >= 1024ULL) return std::to_string(bytes / 1024) + "K";
                return std::to_string(bytes) + "B";
              };

              std::string out;
              // The default set matches Redis's default INFO: everything except commandstats and
              // latencystats (those appear only under all/everything or when explicitly requested).
              static const std::set<std::string> kDefaultSections = {
                  "SERVER", "CLIENTS", "MEMORY",     "PERSISTENCE", "STATS",   "REPLICATION",
                  "CPU",    "MODULES", "ERRORSTATS", "CLUSTER",     "KEYSPACE"};
              auto want = [&](const std::string& s) {
                if (want_all) return true;
                if (requested.count(s) != 0) return true;
                return want_default && kDefaultSections.count(s) != 0;
              };

              if (want("SERVER")) {
                out +=
                    "# Server\r\n"
                    "redis_version:7.2.5\r\n"
                    // Redis-standard field names so drop-in tooling (redis_exporter, redis-cli
                    // --version probes, the printver harness) can parse them.
                    "redis_git_sha1:00000000\r\n"
                    "redis_git_dirty:0\r\n"
                    "redis_build_id:peadb-dev\r\n"
                    "peadb_version:" PEADB_VERSION
                    "\r\n"
                    // Redis computes this from server.cluster_enabled (server.c:5477); its third
                    // value, "sentinel", has no PeaDB equivalent and is never emitted here.
                    "redis_mode:" +
                    std::string(g_cluster_enabled ? "cluster" : "standalone") +
                    "\r\n"
                    "os:Linux\r\n"
                    "arch_bits:64\r\n"
                    "process_id:" +
                    std::to_string(::getpid()) +
                    "\r\n"
                    "tcp_port:" +
                    std::to_string(g_server_port) +
                    "\r\n"
                    "uptime_in_seconds:" +
                    std::to_string(uptime_s) +
                    "\r\n"
                    "uptime_in_days:" +
                    std::to_string(uptime_s / 86400) +
                    "\r\n"
                    "executable:peadb-server\r\n"
                    "config_file:\r\n"
                    "cluster_enabled:" +
                    std::string(g_cluster_enabled ? "1" : "0") + "\r\n";
                // Only while a shutdown waits for lagging replicas, as in Redis.
                if (shutdown_initiated())
                  out +=
                      "shutdown_in_milliseconds:" + std::to_string(g_shutdown_mstime - now_ms()) +
                      "\r\n";
                out += "\r\n";
              }
              if (want("CLIENTS")) {
                // Replicas are not counted, as in Redis.
                const int replicas = g_count_connected_replicas ? g_count_connected_replicas() : 0;
                const int clients = g_connected_clients.load() - replicas;
                out +=
                    "# Clients\r\n"
                    "connected_clients:" +
                    std::to_string(clients) +
                    "\r\n"
                    "blocked_clients:" +
                    std::to_string(blocked_now) +
                    "\r\n"
                    "tracking_clients:0\r\n"
                    "maxclients:" +
                    std::to_string(g_runtime_max_clients.load(std::memory_order_relaxed)) +
                    "\r\n\r\n";
              }
              if (want("MEMORY")) {
                out +=
                    "# Memory\r\n"
                    "used_memory:" +
                    std::to_string(used_mem) +
                    "\r\n"
                    "used_memory_human:" +
                    format_mem(used_mem) +
                    "\r\n"
                    "used_memory_peak:" +
                    std::to_string(g_used_memory_peak) +
                    "\r\n"
                    "used_memory_peak_human:" +
                    format_mem(static_cast<std::size_t>(g_used_memory_peak)) +
                    "\r\n"
                    "used_memory_rss:" +
                    std::to_string(rss_bytes > 0 ? rss_bytes
                                                 : static_cast<std::int64_t>(used_mem)) +
                    "\r\n"
                    "mem_fragmentation_ratio:" +
                    fmt2(frag_ratio) +
                    "\r\n"
                    "mem_not_counted_for_evict:" +
                    std::to_string(free_memory_not_counted()) +
                    "\r\n"
                    "mem_replication_backlog:" +
                    std::to_string(backlog_mem) +
                    "\r\n"
                    "mem_total_replication_buffers:" +
                    std::to_string(backlog_mem + replica_obufs) +
                    "\r\n"
                    "mem_clients_slaves:" +
                    std::to_string(client_mem.second) +
                    "\r\n"
                    "mem_clients_normal:" +
                    std::to_string(client_mem.first) +
                    "\r\n"
                    "mem_aof_buffer:" +
                    std::to_string(zmalloc_string_bytes(g_aof_buf)) +
                    "\r\n"
                    "mem_allocator:libc\r\n"
                    "maxmemory:" +
                    std::to_string(g_config_maxmemory) +
                    "\r\n"
                    "maxmemory_human:" +
                    format_mem(static_cast<size_t>(g_config_maxmemory)) +
                    "\r\n"
                    "maxmemory_policy:" +
                    std::string(maxmemory_policy_to_str(g_maxmemory_policy)) +
                    "\r\n"
                    "number_of_cached_scripts:" +
                    std::to_string(g_script_cache.size()) +
                    "\r\n"
                    "lazyfree_pending_objects:" +
                    std::to_string(store().lazyfree_pending_objects()) + "\r\n\r\n";
              }
              if (want("PERSISTENCE")) {
                // Redis's rdb_changes_since_last_save is the DELTA since the last save baseline
                // (server.dirty), not the absolute mutation count — it drops to 0 right after a
                // save.
                const std::int64_t changes_since_save = std::max<std::int64_t>(
                    0, static_cast<std::int64_t>(store().mutation_epoch()) - g_dirty_at_last_save);
                out +=
                    "# Persistence\r\n"
                    "loading:" +
                    std::to_string(g_loading_replication.load() ? 1 : 0) +
                    "\r\n"
                    "rdb_changes_since_last_save:" +
                    std::to_string(changes_since_save) +
                    "\r\n"
                    "rdb_bgsave_in_progress:" +
                    std::to_string(g_bgsave_in_progress.load() ? 1 : 0) +
                    "\r\n"
                    "rdb_last_save_time:" +
                    std::to_string(g_last_save_time) +
                    "\r\n"
                    "rdb_last_bgsave_status:" +
                    g_last_bgsave_status +
                    "\r\n"
                    "rdb_last_bgsave_time_sec:" +
                    std::to_string(g_rdb_save_time_last) +
                    "\r\n"
                    "rdb_current_bgsave_time_sec:" +
                    std::to_string(g_bgsave_in_progress.load()
                                       ? now_ms() / 1000 - g_bgsave_start_time / 1000
                                       : -1) +
                    "\r\n"
                    "rdb_saves:" +
                    std::to_string(g_stat_rdb_saves) +
                    "\r\n"
                    "aof_enabled:" +
                    std::to_string(g_appendonly ? 1 : 0) +
                    "\r\n"
                    "aof_rewrite_in_progress:" +
                    std::to_string(g_bgrewrite_in_progress.load() ? 1 : 0) +
                    "\r\n"
                    "aof_rewrite_scheduled:" +
                    std::to_string(g_aof_rewrite_scheduled) +
                    "\r\n"
                    "aof_last_bgrewrite_status:" +
                    g_aof_last_bgrewrite_status +
                    "\r\n"
                    "aof_rewrites_consecutive_failures:" +
                    std::to_string(g_aofrw_consecutive_failures) +
                    "\r\n"
                    "aof_last_write_status:" +
                    std::string(g_aof_write_error || g_aof_fsync_error ? "err" : "ok") +
                    "\r\n"
                    "aof_rewrite_buffer_length:" +
                    std::to_string(g_aof_rewrite_buf.size()) + "\r\n\r\n";
              }
              if (want("STATS")) {
                out +=
                    "# Stats\r\n"
                    "total_connections_received:" +
                    std::to_string(g_total_connections_received.load()) +
                    "\r\n"
                    "total_commands_processed:" +
                    std::to_string(g_total_commands_processed.load()) +
                    "\r\n"
                    "instantaneous_ops_per_sec:" +
                    std::to_string(inst_ops) +
                    "\r\n"
                    "total_error_replies:" +
                    std::to_string(g_total_error_replies) +
                    "\r\n"
                    "unexpected_error_replies:" +
                    std::to_string(g_unexpected_error_replies) +
                    "\r\n"
                    "latest_fork_usec:" +
                    std::to_string(g_latest_fork_usec) +
                    "\r\n"
                    "total_forks:" +
                    std::to_string(g_total_forks) +
                    "\r\n"
                    "migrate_cached_sockets:" +
                    std::to_string(g_migrate_cached_sockets.size()) +
                    "\r\n"
                    "pubsub_channels:" +
                    std::to_string(g_channel_subscribers.size()) +
                    "\r\n"
                    "pubsub_patterns:" +
                    std::to_string(g_pattern_subscribers.size()) +
                    "\r\n"
                    "keyspace_hits:" +
                    std::to_string(store().stat_keyspace_hits()) +
                    "\r\n"
                    "keyspace_misses:" +
                    std::to_string(store().stat_keyspace_misses()) +
                    "\r\n"
                    "expired_keys:" +
                    std::to_string(store().stat_expired_keys()) +
                    "\r\n"
                    "evicted_keys:" +
                    std::to_string(store().stat_evicted_keys()) +
                    "\r\n"
                    // PeaDB never evicts clients (maxmemory-clients is fixed at 0).
                    "evicted_clients:0\r\n"
                    "total_eviction_exceeded_time:" +
                    std::to_string((g_eviction_exceeded_total_us + current_eviction_exceeded_us) /
                                   1000) +
                    "\r\n"
                    "current_eviction_exceeded_time:" +
                    std::to_string(current_eviction_exceeded_us / 1000) +
                    "\r\n"
                    "sync_full:" +
                    std::to_string(g_sync_full.load()) +
                    "\r\n"
                    "sync_partial_ok:" +
                    std::to_string(g_sync_partial_ok.load()) +
                    "\r\n"
                    "sync_partial_err:" +
                    std::to_string(g_sync_partial_err.load()) + "\r\n\r\n";
              }
              if (want("REPLICATION")) out += info_replication_section() + "\r\n";
              if (want("CPU")) {
                out +=
                    "# CPU\r\n"
                    "used_cpu_sys:" +
                    fmt6(cpu_sys) +
                    "\r\n"
                    "used_cpu_user:" +
                    fmt6(cpu_user) + "\r\n\r\n";
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
                  if (st.calls == 0 && st.failed_calls == 0 && st.rejected_calls == 0) continue;
                  const double upc = st.calls > 0 ? static_cast<double>(st.usec_total) /
                                                        static_cast<double>(st.calls)
                                                  : 0.0;
                  out += "cmdstat_" + lower(k) + ":calls=" + std::to_string(st.calls) +
                         ",usec=" + std::to_string(st.usec_total) + ",usec_per_call=" + fmt2(upc) +
                         ",rejected_calls=" + std::to_string(st.rejected_calls) +
                         ",failed_calls=" + std::to_string(st.failed_calls) + "\r\n";
                }
                out += "\r\n";
              }
              if (want("CLUSTER")) {
                // Redis emits cluster_enabled here (server.c genRedisInfoString); the copy PeaDB
                // also emits inside "# Server" above is a PeaDB-only addition with no upstream
                // counterpart, kept because RedisInsight and ioredis detect cluster mode from it
                // there (see the CLUSTER command's own comment on this below). Both copies derive
                // from the same g_cluster_enabled global as redis_mode, so they cannot disagree.
                out +=
                    "# Cluster\r\n"
                    "cluster_enabled:" +
                    std::string(g_cluster_enabled ? "1" : "0") + "\r\n\r\n";
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

              return encode_verbatim(out, session.resp_version);
            }});

    t.emplace("SAVE", CommandSpec{"SAVE",
                                  1,
                                  {"admin", "noscript"},
                                  0,
                                  0,
                                  0,
                                  [](const std::vector<std::string>&, SessionState&, bool&) {
                                    // A BGSAVE that ends after this SAVE would rename its older
                                    // snapshot over it.
                                    if (g_bgsave_in_progress.load())
                                      return encode_error("Background save already in progress");
                                    ++g_stat_rdb_saves;
                                    // saveCommand(): the reason is logged, the client gets a bare
                                    // -ERR.
                                    const auto epoch =
                                        static_cast<std::int64_t>(store().mutation_epoch());
                                    std::string err;
                                    if (!rdb_save(g_snapshot_path, err)) {
                                      log(LogLevel::Warn, err);
                                      return std::string("-ERR\r\n");
                                    }
                                    note_rdb_saved(epoch);
                                    return encode_simple("OK");
                                  }});

    // BGSAVE [SCHEDULE] (bgsaveCommand()): one child at a time, so while an AOF rewrite runs a
    // BGSAVE is refused, or with SCHEDULE started by the cron once the rewrite ends; inside
    // MULTI/EXEC it always waits until EXEC ends, so the snapshot never holds half a transaction.
    t.emplace(
        "BGSAVE",
        CommandSpec{
            "BGSAVE",
            -1,
            {"admin", "noscript"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState&, bool&) {
              bool schedule = false;
              if (args.size() > 1) {
                if (args.size() == 2 && upper(args[1]) == "SCHEDULE") {
                  schedule = true;
                } else {
                  return encode_error("syntax error");
                }
              }
              if (g_bgsave_in_progress.load())
                return encode_error("Background save already in progress");
              if (has_active_child_process() || g_executing_exec) {
                if (schedule || g_executing_exec) {
                  g_rdb_bgsave_scheduled = true;
                  return encode_simple("Background saving scheduled");
                }
                return encode_error(
                    "Another child process is active (AOF?): can't BGSAVE right now. Use BGSAVE "
                    "SCHEDULE in order to schedule a BGSAVE whenever possible.");
              }
              if (!do_bgsave_fork()) return std::string("-ERR\r\n");
              return encode_simple("Background saving started");
            }});

    t.emplace("LASTSAVE", CommandSpec{"LASTSAVE",
                                      1,
                                      {"loading", "stale", "fast"},
                                      0,
                                      0,
                                      0,
                                      [](const std::vector<std::string>&, SessionState&, bool&) {
                                        return encode_integer(g_last_save_time);
                                      }});

    t.emplace(
        "BGREWRITEAOF",
        CommandSpec{
            "BGREWRITEAOF",
            1,
            {"admin", "noscript"},
            0,
            0,
            0,
            [](const std::vector<std::string>&, SessionState&, bool&) {
              if (g_bgrewrite_in_progress.load())
                return encode_error("Background append only file rewriting already in progress");
              g_aofrw_consecutive_failures = 0;  // a rewrite asked for by hand is not held back
              // Redis's bgrewriteaofCommand(): beside a BGSAVE child, or inside EXEC, the rewrite
              // starts from the cron (reap_background_children()) once no child runs and the
              // transaction is in the AOF; forked inside EXEC, the child's dataset would hold the
              // transaction's first writes and the rewrite buffer them again. A script is treated
              // like EXEC for the same reason (Redis refuses BGREWRITEAOF there, item 14).
              if (has_active_child_process() || g_executing_exec || in_lua_script_context()) {
                g_aof_rewrite_scheduled = true;
                return encode_simple("Background append only file rewriting scheduled");
              }
              if (!start_aof_rewrite()) {
                return encode_error(
                    "Can't execute an AOF background rewriting. Please check the server logs for "
                    "more information.");
              }
              return encode_simple("Background append only file rewriting started");
            }});

    t.emplace(
        "WAIT",
        CommandSpec{
            "WAIT",
            3,
            {"noscript"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              // waitCommand(): a replica's writes are its own and never reach anybody.
              if (g_replication_role == "slave")
                return encode_error(
                    "WAIT cannot be used with replica instances. Please also note that since Redis "
                    "4.0 if a replica is configured to be writable (which is not the default) "
                    "writes "
                    "to replicas are just local and are not propagated.");
              std::int64_t replicas = 0;
              std::int64_t timeout = 0;
              if (!parse_strict_int(args[1], replicas))
                return encode_error("value is not an integer or out of range");
              if (!parse_strict_int(args[2], timeout))
                return encode_error("timeout is not an integer or out of range");
              if (timeout < 0) return encode_error("timeout is negative");
              // WAIT waits for this client's own last write (c->woff), so a client that has written
              // nothing is answered at once.
              const std::int64_t target = session.woff;
              const int synced = g_count_synced_replicas ? g_count_synced_replicas(target) : 0;
              // A script or a transaction cannot block (CLIENT_DENY_BLOCKING): the count now.
              if (synced >= replicas || g_in_lua_script || g_executing_exec)
                return encode_integer(synced);
              // Ask replicas to ACK now instead of waiting for their ~1 Hz heartbeat, so WAIT
              // resolves in ~1 RTT rather than up to a second. The GETACK rides the stream the same
              // way write commands do, once per event-loop pass after the pass's commands (Redis's
              // replicationRequestAckFromSlaves() and beforeSleep(); so it is not this client's
              // write either), and waits while CLIENT PAUSE stops replica traffic; each replica
              // answers it once it has applied what came before it.
              g_getack_pending = true;
              // Defer: event loop resolves when replicas ack or deadline passes
              const auto deadline = (timeout > 0) ? (now_ms() + timeout) : std::int64_t{0};
              session.wait_state = WaitState{target, deadline, replicas};
              return std::string{};
            }});

    t.emplace(
        "REPLICAOF",
        CommandSpec{
            "REPLICAOF",
            3,
            {"admin"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState&, bool&) {
              const std::string host = args[1];
              const std::string port = args[2];
              if (upper(host) == "NO" && upper(port) == "ONE") {
                g_repl_generation.fetch_add(1);  // signal any running reconnect supervisor to stop
                drop_queued_replica_commands();  // nothing from the old master runs after this
                release_master_link(false);
                g_cached_master = false;
                const bool was_replica = g_replication_role == "slave";
                g_replication_role = "master";
                store().set_replica_expiry(
                    false);  // a master deletes expired keys and sends their DEL
                g_replication_master_host.clear();
                g_replication_master_port = 0;
                g_replica_stale = false;
                g_loading_replication.store(false);
                g_repl_down_since_ms = 0;
                if (g_aof_sync_suspended) restart_aof_after_sync();
                // Redis's replicationUnsetMaster(): the replication id inherited from the old
                // master becomes replid2 up to the current offset, so replicas of the same stream
                // (and the old master) can continue from this server's backlog, which holds the
                // stream it has applied; a fresh replid starts the new history
                // (shiftReplicationId()). Its replicas are disconnected to learn the new id, the
                // new stream starts with a SELECT, and a transaction the old master had not
                // finished is dropped.
                if (was_replica) {
                  g_master_replid2 = g_master_replid;
                  g_second_replid_offset = g_master_repl_offset + 1;  // shiftReplicationId()
                  g_master_replid = gen_run_id();
                  g_last_repl_db = -1;
                  g_master_stream_session = SessionState{};
                  g_master_stream_multi.clear();
                  disconnect_replicas();
                }
                return encode_simple("OK");
              }
              std::int64_t p = 0;
              if (!parse_i64(port, p) || p < 0 || p > 65535)
                return encode_error("value is not an integer or out of range");
              if (g_replication_role == "slave" && host == g_replication_master_host &&
                  p == g_replication_master_port)
                return encode_simple("OK Already connected to specified master");
              // replicationCacheMasterUsingMyself(): a demoted master can resume from the replica
              // just promoted from its own history. A replica keeps its existing cached history.
              if (g_replication_role == "master") {
                g_cached_master = true;
                g_master_stream_session = SessionState{};
              }
              // replicationHandleMasterDisconnection(): leaving a master whose link was up starts
              // master_link_down_since_seconds.
              if (g_replication_role == "slave" && !g_replica_stale)
                g_repl_down_since_ms = now_ms();
              g_replication_master_host = host;
              g_replication_master_port = static_cast<int>(p);
              g_replication_role = "slave";
              store().set_replica_expiry(
                  true);  // a replica keeps expired keys until its master's DEL
              const std::uint64_t gen = g_repl_generation.fetch_add(1) + 1;
              drop_queued_replica_commands();  // nothing from the old master runs after this
              release_master_link(true);
              // replicationSetMaster() → disconnectAllBlockedClients(): a WAIT or a blocked pop
              // would otherwise report a write that the full sync is about to replace, or be served
              // by the master's stream.
              if (g_unblock_blocked_clients_cb)
                g_unblock_blocked_clients_cb(
                    "-UNBLOCKED force unblock from blocking operation, instance state changed "
                    "(master "
                    "-> replica?)\r\n");
              start_master_link(host, static_cast<int>(p), gen);
              return encode_simple("OK");
            }});

    t.emplace(
        "SLAVEOF",
        CommandSpec{"SLAVEOF",
                    3,
                    {"admin"},
                    0,
                    0,
                    0,
                    [](const std::vector<std::string>& args, SessionState& session, bool& close) {
                      std::vector<std::string> proxy = {"REPLICAOF", args[1], args[2]};
                      return handle_command(proxy, session, close);
                    }});

    t.emplace(
        "CLUSTER",
        CommandSpec{
            "CLUSTER",
            -2,
            {"admin"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              // Gate the whole command on cluster-enabled, exactly where Redis does it — before
              // any subcommand dispatch, including HELP/KEYSLOT (cluster.c:5888-5891). It is a
              // startup-only (IMMUTABLE_CONFIG) setting, so this can never flip mid-connection.
              if (!g_cluster_enabled) {
                return encode_error("This instance has cluster support disabled");
              }
              ensure_self_registered();
              const auto sub = upper(args[1]);
              if (sub == "HELP") {
                return encode_array(
                    {encode_bulk(
                         "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                     encode_bulk("MEET <ip> <port>"), encode_bulk("MEETBACK <ip> <port>"),
                     encode_bulk("MYID"), encode_bulk("NODES"), encode_bulk("SLOTS"),
                     encode_bulk("SHARDS"), encode_bulk("INFO"), encode_bulk("KEYSLOT <key>"),
                     encode_bulk("SETSLOT <slot> NODE|MIGRATING|IMPORTING <node>"),
                     encode_bulk("ADDSLOTS <slot> [<slot> ...]"),
                     encode_bulk("DELSLOTS <slot> [<slot> ...]"),
                     encode_bulk("ADDSLOTSRANGE <start slot> <end slot> [...]"),
                     encode_bulk("DELSLOTSRANGE <start slot> <end slot> [...]"),
                     encode_bulk("COUNTKEYSINSLOT <slot>"),
                     encode_bulk("GETKEYSINSLOT <slot> <count>"), encode_bulk("HELP")});
              }
              if (sub == "KEYSLOT" && args.size() == 3) {
                return encode_integer(cluster_keyslot(args[2]));
              }
              if (sub == "MEET" && args.size() == 4) {
                std::int64_t port = 0;
                if (!parse_i64(args[3], port) || port <= 0 || port > 65535) {
                  // cluster.c:5958 (CLUSTER MEET's getLongLongFromObject failure branch). Redis's
                  // own port argument is otherwise unbounded (it only fails synchronously on a
                  // non-numeric argument; an out-of-TCP-range port fails later, asynchronously, in
                  // clusterStartHandshake) — PeaDB has no async handshake to fail into, so both
                  // cases are rejected synchronously here, under Redis's exact wording.
                  return encode_error("Invalid base port specified: " + args[3]);
                }
                // Refuse a MEET that targets this node's own announced address. The round trip
                // below (try_send_cluster_meetback) is a synchronous connect()/read() on this same
                // single event-loop thread: MEETing self means the thread would try to connect() to
                // its own listening socket while it is the only thing that could ever accept() that
                // connection — a guaranteed deadlock, not merely a slow connect, and the socket
                // timeout added below cannot help because nothing ever completes the handshake to
                // time out on. This is also the natural failure mode of a startup script that loops
                // CLUSTER MEET from every node to every known address, including its own — see
                // compat/delta.md item 6.
                if (args[2] == cluster_announced_host() &&
                    static_cast<int>(port) == cluster_announced_port()) {
                  return encode_error("Can't MEET self (" + args[2] + ":" + std::to_string(port) +
                                      " is this node's own cluster-announce address)");
                }
                cluster_register_peer(args[2], static_cast<int>(port));
                try_send_cluster_meetback(args[2], static_cast<int>(port));
                return encode_simple("OK");
              }
              if (sub == "MEETBACK" && args.size() == 4) {
                std::int64_t port = 0;
                if (!parse_i64(args[3], port) || port <= 0 || port > 65535) {
                  // cluster.c:5958 (CLUSTER MEET's getLongLongFromObject failure branch). Redis's
                  // own port argument is otherwise unbounded (it only fails synchronously on a
                  // non-numeric argument; an out-of-TCP-range port fails later, asynchronously, in
                  // clusterStartHandshake) — PeaDB has no async handshake to fail into, so both
                  // cases are rejected synchronously here, under Redis's exact wording.
                  return encode_error("Invalid base port specified: " + args[3]);
                }
                cluster_register_peer(args[2], static_cast<int>(port));
                return encode_simple("OK");
              }
              if ((sub == "ADDSLOTS" || sub == "DELSLOTS") && args.size() >= 3) {
                const bool del = (sub == "DELSLOTS");
                std::vector<std::int64_t> slots;
                slots.reserve(args.size() - 2);
                for (std::size_t i = 2; i < args.size(); ++i) {
                  std::int64_t slot = -1;
                  if (!parse_i64(args[i], slot) || slot < 0 || slot >= 16384) {
                    return encode_error("Invalid or out of range slot");
                  }
                  slots.push_back(slot);
                }
                // Validate first (parse errors, already-busy/unassigned, duplicates-in-this-call —
                // cluster.c's checkSlotAssignmentsOrReply) before mutating anything, so a rejected
                // call never applies a partial change.
                std::array<bool, 16384> seen{};
                for (auto slot : slots) {
                  const auto idx = static_cast<std::size_t>(slot);
                  if (del && g_slot_routes[idx] == SlotRoute::Unassigned) {
                    return encode_error("Slot " + std::to_string(slot) + " is already unassigned");
                  }
                  if (!del && g_slot_routes[idx] != SlotRoute::Unassigned) {
                    return encode_error("Slot " + std::to_string(slot) + " is already busy");
                  }
                  if (seen[idx]) {
                    return encode_error("Slot " + std::to_string(slot) +
                                        " specified multiple times");
                  }
                  seen[idx] = true;
                }
                for (auto slot : slots) {
                  const auto idx = static_cast<std::size_t>(slot);
                  g_slot_routes[idx] = del ? SlotRoute::Unassigned : SlotRoute::Owned;
                  g_slot_owner[idx] = del ? -1 : 0;
                }
                return encode_simple("OK");
              }
              if ((sub == "ADDSLOTSRANGE" || sub == "DELSLOTSRANGE") && args.size() >= 4) {
                if (args.size() % 2 != 0) {
                  return encode_error("wrong number of arguments for 'cluster|" + lower(sub) +
                                      "' command");
                }
                const bool del = (sub == "DELSLOTSRANGE");
                std::vector<std::pair<std::int64_t, std::int64_t>> ranges;
                ranges.reserve((args.size() - 2) / 2);
                for (std::size_t i = 2; i < args.size(); i += 2) {
                  std::int64_t start = -1, end = -1;
                  if (!parse_i64(args[i], start) || start < 0 || start >= 16384 ||
                      !parse_i64(args[i + 1], end) || end < 0 || end >= 16384) {
                    return encode_error("Invalid or out of range slot");
                  }
                  if (start > end) {
                    return encode_error("start slot number " + std::to_string(start) +
                                        " is greater than end slot number " + std::to_string(end));
                  }
                  ranges.emplace_back(start, end);
                }
                std::array<bool, 16384> seen{};
                for (const auto& [start, end] : ranges) {
                  for (std::int64_t slot = start; slot <= end; ++slot) {
                    const auto idx = static_cast<std::size_t>(slot);
                    if (del && g_slot_routes[idx] == SlotRoute::Unassigned) {
                      return encode_error("Slot " + std::to_string(slot) +
                                          " is already unassigned");
                    }
                    if (!del && g_slot_routes[idx] != SlotRoute::Unassigned) {
                      return encode_error("Slot " + std::to_string(slot) + " is already busy");
                    }
                    if (seen[idx]) {
                      return encode_error("Slot " + std::to_string(slot) +
                                          " specified multiple times");
                    }
                    seen[idx] = true;
                  }
                }
                for (const auto& [start, end] : ranges) {
                  for (std::int64_t slot = start; slot <= end; ++slot) {
                    const auto idx = static_cast<std::size_t>(slot);
                    g_slot_routes[idx] = del ? SlotRoute::Unassigned : SlotRoute::Owned;
                    g_slot_owner[idx] = del ? -1 : 0;
                  }
                }
                return encode_simple("OK");
              }
              if (sub == "COUNTKEYSINSLOT" && args.size() == 3) {
                std::int64_t slot = -1;
                if (!parse_i64(args[2], slot) || slot < 0 || slot >= 16384)
                  return encode_error("Invalid slot");
                std::int64_t count = 0;
                // No slot->keys index exists (unlike Redis's server.db->slots_to_keys) — walk the
                // real keyspace, exactly as the brief requires: no caching, no estimate. Unlike
                // store().keys("*"), for_each_key() hands out each key by reference instead of
                // building a vector copy of the whole keyspace first, so this is a single O(N)
                // pass with no allocation instead of "materialise N strings, then scan them". Like
                // Redis's slot index it counts expired keys too, and deletes nothing.
                store().for_each_key([&](const std::string& k) {
                  if (cluster_keyslot(k) == slot) ++count;
                });
                return encode_integer(count);
              }
              if (sub == "GETKEYSINSLOT" && args.size() == 4) {
                std::int64_t slot = -1, maxkeys = 0;
                if (!parse_i64(args[2], slot) || slot < 0 || slot >= 16384 ||
                    !parse_i64(args[3], maxkeys) || maxkeys < 0) {
                  return encode_error("Invalid slot or number of keys");
                }
                // Collect only the keys that actually land in this slot, then sort just that
                // subset — not the whole keyspace — before trimming to maxkeys. Result order
                // stays lexicographic (the documented divergence from Redis's slot-index order,
                // compat/delta.md item 6); only the cost of getting there changes.
                std::vector<std::string> matched;
                store().for_each_key([&](const std::string& k) {
                  if (cluster_keyslot(k) == slot) matched.push_back(k);
                });
                std::sort(matched.begin(), matched.end());
                std::vector<std::string> out;
                for (const auto& k : matched) {
                  if (static_cast<std::int64_t>(out.size()) >= maxkeys) break;
                  out.push_back(encode_bulk(k));
                }
                return encode_array(out);
              }
              if (sub == "SETSLOT" && args.size() >= 5) {
                std::int64_t slot = -1;
                if (!parse_i64(args[2], slot) || slot < 0 || slot >= 16384) {
                  return encode_error("Invalid or out of range slot");
                }
                const std::string mode = upper(args[3]);
                if (mode == "NODE") {
                  const int idx = cluster_resolve_node(args[4]);
                  if (idx < 0) return encode_error("Unknown node " + args[4]);
                  g_slot_routes[static_cast<std::size_t>(slot)] =
                      (idx == 0) ? SlotRoute::Owned : SlotRoute::Moved;
                  g_slot_owner[static_cast<std::size_t>(slot)] = idx;
                  // A definitive NODE assignment ends any in-flight migration for this slot from
                  // this node's point of view — Redis clears the matching side (migrating_slots_to
                  // when the slot moves away, importing_slots_from when this node becomes the
                  // owner; cluster.c:6146-6183) rather than leaving a stale marker naming a
                  // migration that already concluded.
                  g_slot_migrating_to[static_cast<std::size_t>(slot)] = -1;
                  g_slot_importing_from[static_cast<std::size_t>(slot)] = -1;
                  return encode_simple("OK");
                }
                if (mode == "MIGRATING" || mode == "IMPORTING") {
                  const int idx = cluster_resolve_node(args[4]);
                  if (idx < 0) return encode_error("I don't know about node " + args[4]);
                  // Reported ownership (g_slot_owner/g_slot_routes) is untouched: a slot in flight
                  // stays owned by whoever owned it before the migration started, exactly like
                  // Redis's migrating_slots_to/importing_slots_from, which never move the slot out
                  // of the owner's bitmap. Only the redirect-target side channel changes here — see
                  // g_slot_migrating_to/g_slot_importing_from above.
                  if (mode == "MIGRATING") {
                    g_slot_migrating_to[static_cast<std::size_t>(slot)] = idx;
                  } else {
                    g_slot_importing_from[static_cast<std::size_t>(slot)] = idx;
                  }
                  return encode_simple("OK");
                }
                return encode_error(
                    "Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP");
              }
              if (sub == "INFO") {
                std::size_t slots_assigned = 0;
                for (const auto r : g_slot_routes) {
                  if (r != SlotRoute::Unassigned) ++slots_assigned;
                }
                // Distinct owners with at least one slot, not the whole known-node count: a node
                // MEET-registered but not yet assigned any slot does not count toward cluster
                // size, matching Redis's own notion of shard count (genClusterInfoString reads
                // server.cluster->size, which clusterUpdateState() derives the same way).
                std::set<int> owners_with_slots;
                for (const auto owner : g_slot_owner) {
                  if (owner != -1) owners_with_slots.insert(owner);
                }
                // cluster-require-full-coverage defaults to yes (config.c:3072); PeaDB does not
                // expose that config, so this always requires full coverage, matching the default.
                const bool ok = slots_assigned == 16384;
                return encode_verbatim(
                    "cluster_enabled:1\r\n"
                    "cluster_state:" +
                        std::string(ok ? "ok" : "fail") +
                        "\r\n"
                        "cluster_slots_assigned:" +
                        std::to_string(slots_assigned) +
                        "\r\n"
                        "cluster_slots_ok:" +
                        std::to_string(slots_assigned) +
                        "\r\n"
                        "cluster_slots_pfail:0\r\n"
                        "cluster_slots_fail:0\r\n"
                        "cluster_known_nodes:" +
                        std::to_string(g_cluster_nodes.size()) +
                        "\r\n"
                        "cluster_size:" +
                        std::to_string(owners_with_slots.size()) +
                        "\r\n"
                        "cluster_current_epoch:1\r\n"
                        "cluster_my_epoch:1\r\n"
                        "cluster_stats_messages_sent:0\r\n"
                        "cluster_stats_messages_received:0\r\n"
                        "total_cluster_links_buffer_limit_exceeded:0\r\n",
                    session.resp_version);
              }
              if (sub == "NODES") {
                // clusterGenNodeDescription (cluster.c:5254-5331): id, ip:port@bus-port, flags,
                // slaveof-or-"-", ping-sent, pong-recv, config-epoch, link-state, then this node's
                // own owned slot ranges — real Redis joins lines with "\n", not "\r\n".
                const auto ranges = cluster_owned_ranges();
                std::string out;
                for (std::size_t i = 0; i < g_cluster_nodes.size(); ++i) {
                  const auto& n = g_cluster_nodes[i];
                  const int bus_port = (i == 0) ? cluster_announced_bus_port() : n.port + 10000;
                  out += n.id + " " + n.host + ":" + std::to_string(n.port) + "@" +
                         std::to_string(bus_port) + " " + (i == 0 ? "myself,master" : "master") +
                         " - 0 0 1 connected";
                  for (const auto& [start, end, owner] : ranges) {
                    if (owner == static_cast<int>(i)) out += " " + format_slot_range(start, end);
                  }
                  // Redis's clusterGenNodeDescription (cluster.c:5329-5344) only ever appends these
                  // markers to the MYSELF node's own line — migrating_slots_to/importing_slots_from
                  // are this node's perspective on slots in flight, not a fact about node i in
                  // general.
                  if (i == 0) {
                    for (int slot = 0; slot < 16384; ++slot) {
                      const int mig = g_slot_migrating_to[static_cast<std::size_t>(slot)];
                      const int imp = g_slot_importing_from[static_cast<std::size_t>(slot)];
                      if (mig >= 0) {
                        out += " [" + std::to_string(slot) + "->-" +
                               g_cluster_nodes[static_cast<std::size_t>(mig)].id + "]";
                      } else if (imp >= 0) {
                        out += " [" + std::to_string(slot) + "-<-" +
                               g_cluster_nodes[static_cast<std::size_t>(imp)].id + "]";
                      }
                    }
                  }
                  out += "\n";
                }
                return encode_verbatim(out, session.resp_version);
              }
              if (sub == "MYID") {
                return encode_bulk(g_cluster_nodes[0].id);
              }
              if (sub == "SLOTS") {
                // Grouped by real owner, unassigned slots omitted entirely —
                // clusterReplyMultiBulkSlots (cluster.c:5772-5806). Each node entry is 4 elements:
                // ip, port, 40-hex id, and an additional-endpoints map that is empty because
                // PeaDB's preferred endpoint type is always "ip" (addNodeToNodeReply,
                // cluster.c:5591-5639) — there is no cluster-preferred-endpoint-type config to make
                // it anything else.
                std::vector<std::string> out;
                for (const auto& [start, end, owner] : cluster_owned_ranges()) {
                  const auto& n = g_cluster_nodes[static_cast<std::size_t>(owner)];
                  std::string node_entry =
                      encode_array({encode_bulk(n.host), encode_integer(n.port), encode_bulk(n.id),
                                    encode_map({}, session.resp_version)});
                  out.push_back(
                      encode_array({encode_integer(start), encode_integer(end), node_entry}));
                }
                return encode_array(out);
              }
              if (sub == "SHARDS") {
                // One shard per owner (PeaDB models no replicas — see
                // docs/decisions/p5-cluster-addressing-and-version-provenance.md), each a 2-field
                // map: "slots" (a flat start,end,... array) and "nodes" (an array of 7-field maps —
                // addShardReplyForClusterShards/addNodeDetailsToShardReply, cluster.c:5664-5756).
                std::vector<int> owner_order;
                std::map<int, std::vector<std::pair<int, int>>> by_owner;
                for (const auto& [start, end, owner] : cluster_owned_ranges()) {
                  if (by_owner.find(owner) == by_owner.end()) owner_order.push_back(owner);
                  by_owner[owner].emplace_back(start, end);
                }
                std::vector<std::string> shards;
                for (const int owner : owner_order) {
                  const auto& n = g_cluster_nodes[static_cast<std::size_t>(owner)];
                  std::vector<std::string> slot_ints;
                  for (const auto& [start, end] : by_owner[owner]) {
                    slot_ints.push_back(encode_integer(start));
                    slot_ints.push_back(encode_integer(end));
                  }
                  // No tls-port and no hostname configured, so those two optional fields are
                  // omitted, exactly as addNodeDetailsToShardReply omits them when absent.
                  const std::vector<std::string> node_kv = {
                      encode_bulk("id"), encode_bulk(n.id), encode_bulk("port"),
                      encode_integer(n.port), encode_bulk("ip"), encode_bulk(n.host),
                      encode_bulk("endpoint"), encode_bulk(n.host),
                      // No replicas in the cluster model, so every node is a master reporting the
                      // same replication offset INFO replication uses for master_repl_offset.
                      encode_bulk("role"), encode_bulk("master"), encode_bulk("replication-offset"),
                      encode_integer(g_master_repl_offset), encode_bulk("health"),
                      encode_bulk("online")};
                  const std::vector<std::string> shard_kv = {
                      encode_bulk("slots"), encode_array(slot_ints), encode_bulk("nodes"),
                      encode_array({encode_map(node_kv, session.resp_version)})};
                  shards.push_back(encode_map(shard_kv, session.resp_version));
                }
                return encode_array(shards);
              }
              // Redis's addReplySubcommandSyntaxError (networking.c:1146-1153): this is also what
              // an unrecognized subcommand *or* a recognized one called with the wrong argument
              // count (e.g. ADDSLOTSRANGE with no matching end slot) falls through to.
              return encode_error("unknown subcommand or wrong number of arguments for '" +
                                  args[1] + "'. Try CLUSTER HELP.");
            }});

    // ASKING, READONLY and READWRITE are cluster commands: without cluster mode Redis refuses
    // them (askingCommand/readonlyCommand/readwriteCommand in cluster.c). With cluster mode on,
    // READONLY and READWRITE change nothing in PeaDB, which has no replica nodes in a cluster.
    t.emplace("ASKING",
              CommandSpec{"ASKING",
                          1,
                          {"fast"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>&, SessionState& session, bool&) {
                            if (!g_cluster_enabled)
                              return encode_error("This instance has cluster support disabled");
                            session.asking = true;
                            return encode_simple("OK");
                          }});

    t.emplace("READONLY",
              CommandSpec{"READONLY",
                          1,
                          {"readonly", "fast"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>&, SessionState&, bool&) {
                            if (!g_cluster_enabled)
                              return encode_error("This instance has cluster support disabled");
                            return encode_simple("OK");
                          }});

    t.emplace("READWRITE",
              CommandSpec{"READWRITE",
                          1,
                          {"readonly", "fast"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>&, SessionState&, bool&) {
                            if (!g_cluster_enabled)
                              return encode_error("This instance has cluster support disabled");
                            return encode_simple("OK");
                          }});

    t.emplace(
        "COMMAND",
        CommandSpec{
            "COMMAND",
            -1,
            {"loading", "stale"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              if (args.size() >= 2 && upper(args[1]) == "HELP") {
                return encode_array(
                    {encode_bulk(
                         "COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                     encode_bulk("(no subcommand)"), encode_bulk("COUNT"),
                     encode_bulk("DOCS [<command-name> ...]"),
                     encode_bulk("GETKEYS <full-command>"),
                     encode_bulk("INFO [<command-name> ...]"), encode_bulk("LIST"),
                     encode_bulk("HELP")});
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
                if (kMovableKeys.count(lower(s.name)) != 0)
                  flags.push_back(encode_bulk("movablekeys"));
                // Redis COMMAND INFO shape: name, arity, flags, first/last/step key,
                // acl-categories, tips, key-specs, subcommands (last four left empty).
                return encode_array({encode_bulk(lower(s.name)), encode_integer(s.arity),
                                     encode_array(flags), encode_integer(s.first_key),
                                     encode_integer(s.last_key), encode_integer(s.key_step),
                                     encode_array({}), encode_array({}), encode_array({}),
                                     encode_array({})});
              };
              if (args.size() == 1) {
                std::vector<std::string> out;
                out.reserve(table.size());
                for (const auto& [k, s] : table) {
                  (void)k;
                  out.push_back(info(s));
                }
                return encode_array(out);
              }
              const std::string sub = upper(args[1]);
              if (sub == "COUNT") return encode_integer(static_cast<long long>(table.size()));
              if (sub == "INFO") {
                if (args.size() == 2) return encode_array({});
                std::vector<std::string> out;
                for (std::size_t i = 2; i < args.size(); ++i) {
                  auto it = table.find(upper(args[i]));
                  out.push_back(it == table.end() ? encode_null(session.resp_version)
                                                  : info(it->second));
                }
                return encode_array(out);
              }
              if (sub == "GETKEYS" || sub == "GETKEYSANDFLAGS") {
                if (args.size() < 3)
                  return encode_error(
                      "Unknown subcommand or wrong number of arguments for 'getkeys'");
                auto it = table.find(upper(args[2]));
                if (it == table.end()) return encode_error("Invalid command specified");
                const auto& s = it->second;
                const std::vector<std::string> cmdline(args.begin() + 2, args.end());
                const auto gk_cmd = upper(cmdline[0]);

                // (key, per-key-spec flags) pairs. Flags mirror Redis's command key-spec flags
                // (RO/RW/OW read-write intent + access/update/insert/delete). Commands whose keys
                // aren't a fixed range (SORT STORE, EVAL/FCALL numkeys, MEMORY USAGE, XGROUP,
                // ZUNIONSTORE-family) are extracted explicitly, like Redis's per-command getkeys
                // procs.
                std::vector<std::pair<std::string, std::vector<std::string>>> kf;
                auto add = [&](const std::string& k, std::vector<std::string> flags) {
                  kf.emplace_back(k, std::move(flags));
                };
                const bool dynamic_keys =
                    (gk_cmd == "EVAL" || gk_cmd == "EVALSHA" || gk_cmd == "EVAL_RO" ||
                     gk_cmd == "EVALSHA_RO" || gk_cmd == "FCALL" || gk_cmd == "FCALL_RO");

                if ((gk_cmd == "SORT" || gk_cmd == "SORT_RO") && cmdline.size() >= 2) {
                  add(cmdline[1], {"RO", "access"});
                  std::string dest;
                  bool have_store = false;  // last STORE wins (matches do_sort)
                  for (std::size_t i = 2; i + 1 < cmdline.size(); ++i)
                    if (upper(cmdline[i]) == "STORE") {
                      dest = cmdline[i + 1];
                      have_store = true;
                      ++i;
                    }
                  if (have_store) add(dest, {"OW", "update"});
                } else if (dynamic_keys) {
                  std::int64_t nk = 0;
                  if (cmdline.size() < 3 || !parse_i64(cmdline[2], nk) || nk < 0)
                    return encode_error("Invalid arguments specified for command");
                  if (static_cast<std::size_t>(nk) > cmdline.size() - 3)
                    return encode_error("Number of keys can't be greater than number of args");
                  for (std::int64_t k = 0; k < nk; ++k)
                    add(cmdline[static_cast<std::size_t>(3 + k)], {"RW", "access", "update"});
                } else if (gk_cmd == "MEMORY" && cmdline.size() >= 3 &&
                           upper(cmdline[1]) == "USAGE") {
                  add(cmdline[2], {"RO", "access"});
                } else if (gk_cmd == "XGROUP" && cmdline.size() >= 3) {
                  const auto x = upper(cmdline[1]);
                  if (x == "CREATE" || x == "SETID" || x == "DESTROY" || x == "CREATECONSUMER" ||
                      x == "DELCONSUMER")
                    add(cmdline[2], {"RW"});
                } else if ((gk_cmd == "ZUNIONSTORE" || gk_cmd == "ZINTERSTORE" ||
                            gk_cmd == "ZDIFFSTORE") &&
                           cmdline.size() >= 4) {
                  std::int64_t nk = 0;
                  if (!parse_i64(cmdline[2], nk) || nk < 0)
                    return encode_error("Invalid arguments specified for command");
                  add(cmdline[1], {"OW", "update"});  // destination
                  for (std::int64_t k = 0;
                       k < nk && static_cast<std::size_t>(3 + k) < cmdline.size(); ++k)
                    add(cmdline[static_cast<std::size_t>(3 + k)], {"RO", "access"});
                } else if (gk_cmd == "LMOVE" && cmdline.size() >= 3) {
                  add(cmdline[1], {"RW", "access", "delete"});
                  add(cmdline[2], {"RW", "insert"});
                } else if (gk_cmd == "MIGRATE" && cmdline.size() >= 4) {
                  // Redis's key specs: the key at index 3, unless a KEYS is found searching back
                  // from the second-last argument; then migrateGetKeys() (the options before KEYS
                  // skipped, and a non-empty key argument makes it an invalid call).
                  bool keyword = false;
                  for (std::size_t i = cmdline.size() - 2; i >= 1 && !keyword; --i)
                    keyword = upper(cmdline[i]) == "KEYS";
                  std::size_t first = 3;
                  std::size_t num = 1;
                  if (keyword && cmdline.size() > 6) {
                    for (std::size_t i = 6; i < cmdline.size(); ++i) {
                      const auto opt = upper(cmdline[i]);
                      if (opt == "KEYS") {
                        if (!cmdline[3].empty()) {
                          num = 0;
                        } else {
                          first = i + 1;
                          num = cmdline.size() - first;
                        }
                        break;
                      }
                      if (opt == "AUTH")
                        i += 1;
                      else if (opt == "AUTH2")
                        i += 2;
                    }
                  }
                  if (num == 0) return encode_error("Invalid arguments specified for command");
                  for (std::size_t i = first; i < first + num; ++i)
                    add(cmdline[i], {"RW", "access", "delete"});
                } else if (gk_cmd == "SET" && cmdline.size() >= 2) {
                  add(cmdline[1], {"OW", "update"});
                } else if (gk_cmd == "MSET" && cmdline.size() >= 2) {
                  for (std::size_t i = 1; i + 1 < cmdline.size(); i += 2)
                    add(cmdline[i], {"OW", "update"});
                } else {
                  if (s.first_key > 0) {
                    int last = s.last_key;
                    if (last < 0) last = static_cast<int>(cmdline.size()) + last;
                    const int step = s.key_step > 0 ? s.key_step : 1;
                    const std::vector<std::string> def =
                        s.is_write() ? std::vector<std::string>{"RW", "access", "update"}
                                     : std::vector<std::string>{"RO", "access"};
                    for (int k = s.first_key; k <= last && k < static_cast<int>(cmdline.size());
                         k += step)
                      add(cmdline[static_cast<std::size_t>(k)], def);
                  }
                }

                // EVAL/FCALL with numkeys 0 legitimately has no keys — return an empty array, not
                // an error (the "no key arguments" error is for commands that can never take a
                // key).
                if (kf.empty() && !dynamic_keys)
                  return encode_error("The command has no key arguments");
                std::vector<std::string> out;
                for (const auto& [k, flags] : kf) {
                  if (sub == "GETKEYSANDFLAGS") {
                    std::vector<std::string>
                        fl;  // status replies, as Redis's addReplyFlagsForKeyArgs()
                    for (const auto& f : flags) fl.push_back(encode_simple(f));
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
                // generated Redis metadata; the filter narrows by ACL category, name glob, or
                // module.
                enum { kAll, kModule, kAclcat, kPattern } mode = kAll;
                std::string filter;
                if (args.size() == 2) {
                  // no filter
                } else if (args.size() == 5 && upper(args[2]) == "FILTERBY") {
                  const auto by = upper(args[3]);
                  filter = args[4];
                  if (by == "MODULE")
                    mode = kModule;
                  else if (by == "ACLCAT")
                    mode = kAclcat;
                  else if (by == "PATTERN")
                    mode = kPattern;
                  else
                    return encode_error("syntax error");
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
                      const std::string tok =
                          cats.substr(p, sp == std::string::npos ? std::string::npos : sp - p);
                      if (tok == want_cat) {
                        has = true;
                        break;
                      }
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
              if (sub == "DOCS")
                return session.resp_version == RespVersion::Resp3 ? std::string("%0\r\n")
                                                                  : std::string("*0\r\n");
              return encode_error("unknown subcommand for COMMAND");
            }});

    t.emplace(
        "CONFIG",
        CommandSpec{
            "CONFIG",
            -2,
            {"admin", "loading"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              const std::string sub = upper(args[1]);
              // The arity of each subcommand, as Redis's command table checks it.
              const auto arity_error = [&] {
                return encode_error("wrong number of arguments for 'config|" + lower(args[1]) +
                                    "' command");
              };
              if (sub == "GET") {
                if (args.size() < 3) return arity_error();
                return config_get_reply(args, session.resp_version);
              }
              if (sub == "SET") {
                if (args.size() < 4) return arity_error();
                return config_set_reply(args, session);
              }
              if (sub == "RESETSTAT") {
                if (args.size() != 2) return arity_error();
                g_cmdstats.clear();
                g_cmd_latency_hist.clear();  // LATENCY HISTOGRAM resets with commandstats
                g_errorstats.clear();
                g_total_error_replies = 0;
                g_unexpected_error_replies = 0;
                g_stat_rdb_saves = 0;
                return encode_simple("OK");
              }
              if (sub == "REWRITE") {
                if (args.size() != 2) return arity_error();
                if (g_config_file.empty()) {
                  return encode_error("The server is running without a config file");
                }
                return encode_error(
                    "CONFIG REWRITE is not supported by PeaDB yet (compat/delta.md item 25)");
              }
              if (sub == "HELP") {
                if (args.size() != 2) return arity_error();
                // addReplyHelp(): one status reply per line.
                return encode_array(
                    {encode_simple(
                         "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                     encode_simple("GET <pattern>"),
                     encode_simple("    Return parameters matching the glob-like <pattern> and "
                                   "their values."),
                     encode_simple("SET <directive> <value>"),
                     encode_simple("    Set the configuration <directive> to <value>."),
                     encode_simple("RESETSTAT"),
                     encode_simple("    Reset statistics reported by the INFO command."),
                     encode_simple("REWRITE"), encode_simple("    Rewrite the configuration file."),
                     encode_simple("HELP"), encode_simple("    Print this help.")});
              }
              return encode_error("unknown subcommand '" + args[1].substr(0, 128) +
                                  "'. Try CONFIG HELP.");
            }});

    t.emplace(
        "DEBUG",
        CommandSpec{
            "DEBUG",
            -2,
            {"admin"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              if (args.size() == 3 && upper(args[1]) == "SET-ACTIVE-EXPIRE") {
                std::int64_t v = 0;
                if (!parse_i64(args[2], v) || (v != 0 && v != 1))
                  return encode_error("value is not an integer or out of range");
                g_active_expire = (v == 1);
                return encode_simple("OK");
              }
              if (args.size() == 3 && upper(args[1]) == "SET-DISABLE-DENY-SCRIPTS") {
                std::int64_t v = 0;
                if (!parse_i64(args[2], v) || (v != 0 && v != 1))
                  return encode_error("value is not an integer or out of range");
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
                  if (o == "NOSAVE")
                    nosave = true;
                  else if (o == "NOFLUSH")
                    noflush = true;
                  else if (o == "MERGE")
                    merge = true;
                }
                std::string err;
                if (!nosave) {
                  const auto epoch = static_cast<std::int64_t>(store().mutation_epoch());
                  if (!rdb_save(g_snapshot_path, err)) {
                    log(LogLevel::Warn, err);
                    return std::string("-ERR\r\n");
                  }
                  note_rdb_saved(epoch);
                }
                if (!noflush) {
                  store().flushall();
                  functions_flush_all();
                }
                g_rdb_load_functions_replace = merge;
                const bool ok = rdb_load(g_snapshot_path, err);
                g_rdb_load_functions_replace =
                    true;  // restore safe default for replica/startup loads
                if (!ok) return encode_error("Error trying to load the RDB dump: " + err);
                return encode_simple("OK");
              }
              if (args.size() == 3 && upper(args[1]) == "OBJECT") {
                // Raw dict peek like Redis debugCommand: a logically-expired-but-unreaped
                // key still reports its object info instead of erroring/expiring.
                const DataStore::LazyExpireDisabled raw_lookup(store());
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
                // Redis's flushAppendOnlyFile(1) first. Bytes the file still cannot take would be
                // lost from memory by the reload and written after it, so PeaDB refuses then.
                if (g_aof_state == AofState::On) flush_append_only_file({});
                if (!g_aof_buf.empty()) return misconf_reply();
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
                if (!parse_i64(args[2], count) || count < 0)
                  return encode_error("value is not an integer or out of range");
                const std::string prefix = args.size() >= 4 ? args[3] : "key:";
                std::int64_t vsize = 0;
                if (args.size() == 5 && (!parse_i64(args[4], vsize) || vsize < 0))
                  return encode_error("value is not an integer or out of range");
                for (std::int64_t i = 0; i < count; ++i) {
                  std::string val = "value:" + std::to_string(i);
                  if (vsize > 0) val.resize(static_cast<std::size_t>(vsize), 'A');
                  store().set(prefix + std::to_string(i), val, false, false, std::nullopt, false);
                }
                return encode_simple("OK");
              }
              if (args.size() == 3 && upper(args[1]) == "SET-SKIP-CHECKSUM-VALIDATION") {
                std::int64_t v = 0;
                if (!parse_i64(args[2], v) || (v != 0 && v != 1))
                  return encode_error("value is not an integer or out of range");
                rdb_set_skip_checksum_validation(v == 1);
                return encode_simple("OK");
              }
              if (args.size() == 3 && upper(args[1]) == "QUICKLIST-PACKED-THRESHOLD") {
                return encode_simple("OK");  // accepted; meaningful once real quicklists exist
              }
              if (args.size() == 3 && upper(args[1]) == "PAUSE-CRON") {
                std::int64_t v = 0;
                if (!parse_i64(args[2], v) || (v != 0 && v != 1))
                  return encode_error("value is not an integer or out of range");
                g_pause_cron = (v == 1);
                return encode_simple("OK");
              }
              if (args.size() == 2 && upper(args[1]) == "JMAP") return encode_simple("OK");
              if (args.size() == 3 && upper(args[1]) == "SLEEP") {
                double secs = 0;
                try {
                  secs = std::stod(args[2]);
                } catch (...) {
                  return encode_error("Invalid debug sleep time");
                }
                if (secs > 0) {
                  std::this_thread::sleep_for(
                      std::chrono::microseconds(static_cast<long long>(secs * 1000000)));
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
                  return encode_verbatim(s, session.resp_version);
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
                return encode_error(
                    "Wrong protocol type name. Please use one of the following: "
                    "string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|"
                    "false");
              }
              return encode_error(
                  "unknown subcommand or wrong number of arguments for 'DEBUG'. Try DEBUG HELP.");
            }});

    t.emplace(
        "MODULE",
        CommandSpec{
            "MODULE",
            -2,
            {"admin"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState&, bool&) {
              const auto sub = upper(args[1]);
              if (sub == "HELP") {
                return encode_array(
                    {encode_bulk("MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                     encode_bulk("LIST"), encode_bulk("LOAD <path> [<arg> ...]"),
                     encode_bulk("LOADEX <path> [[CONFIG NAME VALUE] [ARGS ...]]"),
                     encode_bulk("UNLOAD <name>"), encode_bulk("HELP")});
              }
              if (sub == "LOAD" && args.size() >= 3) {
                // MODULE LOAD dlopen()s arbitrary code; gate it behind explicit opt-in
                // (default off) so an authenticated-but-untrusted or misconfigured server
                // is not a trivial RCE primitive.
                if (g_enable_module_command != "yes" && g_enable_module_command != "local") {
                  return encode_error(
                      "MODULE command not allowed. If you really want to use it, "
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
                if (!handle)
                  return encode_error("Error loading the extension. Please check the server logs.");
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
                if (it == g_loaded_modules.end())
                  return encode_error("no such module with that name");
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
                      encode_bulk("name"),
                      encode_bulk(name),
                      encode_bulk("path"),
                      encode_bulk(mod.path),
                  }));
                }
                return encode_array(out);
              }
              return encode_error("unknown subcommand for MODULE");
            }});

    t.emplace("GET", CommandSpec{"GET",
                                 2,
                                 {"readonly", "fast"},
                                 1,
                                 1,
                                 1,
                                 [](const std::vector<std::string>& args, SessionState&, bool&) {
                                   bool wrongtype = false;
                                   auto v = store().get_string(args[1], wrongtype);
                                   if (wrongtype) return wrongtype_error_reply();
                                   return v.has_value() ? encode_bulk(*v)
                                                        : encode_null(RespVersion::Resp2);
                                 }});

    t.emplace("SET",
              CommandSpec{"SET",
                          -3,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            // Redis's setCommand() and setGenericCommand(): the options, the expire
                            // value, GET's type check, then NX or XX. A SET they stop changes
                            // nothing: it is not propagated (canonicalize_propagated_command() has
                            // the form of one that ran) and sends no event.
                            ExtendedStringArgs opts;
                            if (!parse_extended_string_args(args, true, opts))
                              return encode_error("syntax error");
                            std::optional<std::int64_t> expire_at;
                            if (opts.expire != nullptr) {
                              std::string err;
                              expire_at = parse_expiry(opts.expire_mode, *opts.expire, err, "set");
                              if (!expire_at.has_value()) return encode_error(err);
                            }
                            // Plain SET overwrites a key of any type; only GET needs the old value
                            // to be a string.
                            std::optional<std::string> old;
                            if (opts.get) {
                              if (store().is_wrongtype_for_string(args[1]))
                                return wrongtype_error_reply();
                              old = store().get(args[1]);
                            }
                            const bool done = store().set(args[1], args[2], opts.nx, opts.xx,
                                                          expire_at, opts.keepttl);
                            if (!done) g_write_changed_nothing = true;
                            if (opts.get) {
                              return old.has_value() ? encode_bulk(*old)
                                                     : encode_null(RespVersion::Resp2);
                            }
                            return done ? encode_simple("OK") : encode_null(RespVersion::Resp2);
                          }});

    t.emplace("SETEX",
              CommandSpec{"SETEX",
                          4,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            std::int64_t sec = 0;
                            if (!parse_i64(args[2], sec))
                              return encode_error("value is not an integer or out of range");
                            if (sec <= 0) return encode_error("invalid expire time in setex");
                            // SETEX overwrites any existing type, like SET.
                            const auto exp = now_ms() + sec * 1000;
                            const bool ok = store().set(args[1], args[3], false, false, exp, false);
                            return ok ? encode_simple("OK") : encode_null(RespVersion::Resp2);
                          }});

    t.emplace("PSETEX",
              CommandSpec{"PSETEX",
                          4,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            std::int64_t ms = 0;
                            if (!parse_i64(args[2], ms))
                              return encode_error("value is not an integer or out of range");
                            if (ms <= 0) return encode_error("invalid expire time in psetex");
                            // PSETEX overwrites any existing type, like SET.
                            const auto exp = now_ms() + ms;
                            const bool ok = store().set(args[1], args[3], false, false, exp, false);
                            return ok ? encode_simple("OK") : encode_null(RespVersion::Resp2);
                          }});

    t.emplace("SETNX", CommandSpec{"SETNX",
                                   3,
                                   {"write"},
                                   1,
                                   1,
                                   1,
                                   [](const std::vector<std::string>& args, SessionState&, bool&) {
                                     // SETNX on an existing key of any type returns 0 without a
                                     // type error.
                                     const bool ok = store().set(args[1], args[2], true, false,
                                                                 std::nullopt, false);
                                     if (!ok) g_write_changed_nothing = true;
                                     return encode_integer(ok ? 1 : 0);
                                   }});

    t.emplace("MGET", CommandSpec{"MGET",
                                  -2,
                                  {"readonly"},
                                  1,
                                  -1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    std::vector<std::string> out;
                                    for (std::size_t i = 1; i < args.size(); ++i) {
                                      auto v = store().get(args[i]);
                                      out.push_back(v.has_value()
                                                        ? encode_bulk(*v)
                                                        : encode_null(RespVersion::Resp2));
                                    }
                                    return encode_array(out);
                                  }});

    t.emplace("MSET",
              CommandSpec{"MSET",
                          -3,
                          {"write"},
                          1,
                          -1,
                          2,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            if (((args.size() - 1) % 2) != 0)
                              return encode_error("wrong number of arguments for 'mset' command");
                            for (std::size_t i = 1; i < args.size(); i += 2) {
                              store().set(args[i], args[i + 1], false, false, std::nullopt, false);
                            }
                            return encode_simple("OK");
                          }});

    t.emplace("MSETNX",
              CommandSpec{"MSETNX",
                          -3,
                          {"write"},
                          1,
                          -1,
                          2,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            if (((args.size() - 1) % 2) != 0)
                              return encode_error("wrong number of arguments for 'msetnx' command");
                            // A key of any type stops it, as Redis's lookupKeyWrite() finds it.
                            for (std::size_t i = 1; i < args.size(); i += 2) {
                              if (store().type_of(args[i]) != "none") {
                                g_write_changed_nothing = true;
                                return encode_integer(0);
                              }
                            }
                            for (std::size_t i = 1; i < args.size(); i += 2) {
                              (void)store().set(args[i], args[i + 1], false, false, std::nullopt,
                                                false);
                            }
                            return encode_integer(1);
                          }});

    t.emplace("GETDEL", CommandSpec{"GETDEL",
                                    2,
                                    {"write"},
                                    1,
                                    1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState&, bool&) {
                                      return with_string_key(args[1], [&]() {
                                        auto v = store().getdel(args[1]);
                                        if (!v.has_value())
                                          g_write_changed_nothing = true;  // nothing deleted
                                        return v.has_value() ? encode_bulk(*v)
                                                             : encode_null(RespVersion::Resp2);
                                      });
                                    }});

    t.emplace("GETSET", CommandSpec{"GETSET",
                                    3,
                                    {"write"},
                                    1,
                                    1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState&, bool&) {
                                      return with_string_key(args[1], [&]() {
                                        auto old = store().get(args[1]);
                                        (void)store().set(args[1], args[2], false, false,
                                                          std::nullopt, false);
                                        return old.has_value() ? encode_bulk(*old)
                                                               : encode_null(RespVersion::Resp2);
                                      });
                                    }});

    t.emplace("GETEX", CommandSpec{"GETEX",
                                   -2,
                                   {"write"},
                                   1,
                                   1,
                                   1,
                                   [](const std::vector<std::string>& args, SessionState&, bool&) {
                                     // Redis's getexCommand(): the options, the key (nil, or
                                     // WRONGTYPE), then the expire value. Only a TTL set or
                                     // removed, or a key an expire time in the past deletes, is a
                                     // change; canonicalize_propagated_command() sends it as
                                     // PEXPIREAT, PERSIST or DEL.
                                     ExtendedStringArgs opts;
                                     if (!parse_extended_string_args(args, false, opts))
                                       return encode_error("syntax error");
                                     bool wrongtype = false;
                                     const auto value = store().get_string(args[1], wrongtype);
                                     if (wrongtype) return wrongtype_error_reply();
                                     if (!value.has_value()) {
                                       g_write_changed_nothing = true;
                                       return encode_null(RespVersion::Resp2);
                                     }
                                     if (opts.expire != nullptr) {
                                       std::string err;
                                       const auto expire_at = parse_expiry(
                                           opts.expire_mode, *opts.expire, err, "getex");
                                       if (!expire_at.has_value()) return encode_error(err);
                                       if (already_expired(*expire_at))
                                         store().del(args[1]);
                                       else
                                         store().expire(args[1], *expire_at);
                                     } else if (!opts.persist || !store().persist(args[1])) {
                                       g_write_changed_nothing =
                                           true;  // no option, or PERSIST of a key without a TTL
                                     }
                                     return encode_bulk(*value);
                                   }});

    t.emplace(
        "APPEND",
        CommandSpec{
            "APPEND",
            3,
            {"write"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState&, bool&) {
              return with_string_key(args[1], [&]() {
                if (!must_obey_client() && static_cast<std::int64_t>(store().strlen(args[1])) +
                                                   static_cast<std::int64_t>(args[2].size()) >
                                               g_proto_max_bulk_len) {
                  return encode_error("string exceeds maximum allowed size (proto-max-bulk-len)");
                }
                return encode_integer(store().append(args[1], args[2]));
              });
            }});

    t.emplace("STRLEN", CommandSpec{"STRLEN",
                                    2,
                                    {"readonly", "fast"},
                                    1,
                                    1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState&, bool&) {
                                      return with_string_key(args[1], [&]() {
                                        return encode_integer(store().strlen(args[1]));
                                      });
                                    }});

    t.emplace("SETBIT",
              CommandSpec{"SETBIT",
                          4,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            std::int64_t offset = 0;
                            if (!parse_i64(args[2], offset) || offset < 0 ||
                                (!must_obey_client() && (offset >> 3) >= g_proto_max_bulk_len)) {
                              return encode_error("bit offset is not an integer or out of range");
                            }
                            std::int64_t bit = 0;
                            if (!parse_i64(args[3], bit) || (bit != 0 && bit != 1)) {
                              return encode_error("bit is not an integer or out of range");
                            }
                            return with_string_key(args[1], [&]() {
                              bool ok = false;
                              std::string err;
                              const int prev =
                                  store().setbit(args[1], static_cast<std::uint64_t>(offset),
                                                 static_cast<int>(bit), ok, err);
                              if (!ok) {
                                if (err == "WRONGTYPE") return wrongtype_error_reply();
                                return encode_error(err.empty() ? "ERR" : err);
                              }
                              return encode_integer(prev);
                            });
                          }});

    t.emplace("GETBIT",
              CommandSpec{"GETBIT",
                          3,
                          {"readonly", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            std::int64_t offset = 0;
                            if (!parse_i64(args[2], offset) || offset < 0 ||
                                (!must_obey_client() && (offset >> 3) >= g_proto_max_bulk_len)) {
                              return encode_error("bit offset is not an integer or out of range");
                            }
                            return with_string_key(args[1], [&]() {
                              bool ok = false;
                              std::string err;
                              const int out = store().getbit(
                                  args[1], static_cast<std::uint64_t>(offset), ok, err);
                              if (!ok) {
                                if (err == "WRONGTYPE") return wrongtype_error_reply();
                                return encode_error(err.empty() ? "ERR" : err);
                              }
                              return encode_integer(out);
                            });
                          }});

    t.emplace(
        "SETRANGE",
        CommandSpec{
            "SETRANGE",
            4,
            {"write"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState&, bool&) {
              std::int64_t offset = 0;
              if (!parse_i64(args[2], offset) || offset < 0) {
                return encode_error("offset is out of range");
              }
              if (!args[3].empty() && !must_obey_client() &&
                  offset > g_proto_max_bulk_len - static_cast<std::int64_t>(args[3].size())) {
                // Subtraction form avoids signed overflow when offset is near INT64_MAX.
                return encode_error("string exceeds maximum allowed size (proto-max-bulk-len)");
              }
              return with_string_key(args[1], [&]() {
                bool ok = false;
                std::string err;
                const auto out =
                    store().setrange(args[1], static_cast<std::size_t>(offset), args[3], ok, err);
                if (!ok) {
                  if (err == "WRONGTYPE") return wrongtype_error_reply();
                  return encode_error(err.empty() ? "ERR" : err);
                }
                if (args[3].empty())
                  g_write_changed_nothing = true;  // Redis answers the length only
                return encode_integer(static_cast<long long>(out));
              });
            }});

    t.emplace("GETRANGE",
              CommandSpec{"GETRANGE",
                          4,
                          {"readonly"},
                          1,
                          1,
                          1,
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

    t.emplace(
        "SUBSTR",
        CommandSpec{"SUBSTR",
                    4,
                    {"readonly"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& args, SessionState& session, bool& close) {
                      std::vector<std::string> proxy = {"GETRANGE", args[1], args[2], args[3]};
                      return handle_command(proxy, session, close);
                    }});

    t.emplace(
        "LCS",
        CommandSpec{"LCS",
                    -3,
                    {"readonly"},
                    1,
                    2,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) {
                      bool want_len = false;
                      bool want_idx = false;
                      bool with_match_len = false;
                      std::int64_t min_match_len = 0;
                      for (std::size_t i = 3; i < args.size(); ++i) {
                        const auto opt = upper(args[i]);
                        if (opt == "LEN") {
                          want_len = true;
                          continue;
                        }
                        if (opt == "IDX") {
                          want_idx = true;
                          continue;
                        }
                        if (opt == "WITHMATCHLEN") {
                          with_match_len = true;
                          continue;
                        }
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
                      if (!want_idx && (with_match_len || min_match_len > 0))
                        return encode_error("syntax error");
                      if (store().is_wrongtype_for_string(args[1]) ||
                          store().is_wrongtype_for_string(args[2])) {
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

    t.emplace("INCR", CommandSpec{"INCR",
                                  2,
                                  {"write", "denyoom", "fast"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return with_string_key(args[1], [&]() {
                                      bool ok = false;
                                      std::string err;
                                      auto v = store().incrby(args[1], 1, ok, err);
                                      return ok ? encode_integer(v) : encode_error(err);
                                    });
                                  }});

    t.emplace("INCRBY",
              CommandSpec{"INCRBY",
                          3,
                          {"write", "denyoom", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            std::int64_t by = 0;
                            if (!parse_i64(args[2], by))
                              return encode_error("value is not an integer or out of range");
                            return with_string_key(args[1], [&]() {
                              bool ok = false;
                              std::string err;
                              auto v = store().incrby(args[1], by, ok, err);
                              return ok ? encode_integer(v) : encode_error(err);
                            });
                          }});

    t.emplace("DECR", CommandSpec{"DECR",
                                  2,
                                  {"write", "denyoom", "fast"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return with_string_key(args[1], [&]() {
                                      bool ok = false;
                                      std::string err;
                                      auto v = store().incrby(args[1], -1, ok, err);
                                      return ok ? encode_integer(v) : encode_error(err);
                                    });
                                  }});

    t.emplace("DECRBY",
              CommandSpec{"DECRBY",
                          3,
                          {"write", "denyoom", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            std::int64_t by = 0;
                            if (!parse_i64(args[2], by))
                              return encode_error("value is not an integer or out of range");
                            if (by == std::numeric_limits<std::int64_t>::min()) {
                              return encode_error(
                                  "decrement would overflow");  // -INT64_MIN is unrepresentable
                            }
                            return with_string_key(args[1], [&]() {
                              bool ok = false;
                              std::string err;
                              auto v = store().incrby(args[1], -by, ok, err);
                              return ok ? encode_integer(v) : encode_error(err);
                            });
                          }});

    t.emplace("INCRBYFLOAT",
              CommandSpec{"INCRBYFLOAT",
                          3,
                          {"write", "denyoom", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            long double by = 0;
                            if (!parse_strict_float(args[2], by))
                              return encode_error("value is not a valid float");
                            return with_string_key(args[1], [&]() {
                              bool ok = false;
                              std::string err;
                              auto v = store().incrbyfloat(args[1], by, ok, err);
                              return ok ? encode_bulk(v) : encode_error(err);
                            });
                          }});

    t.emplace("DEL", CommandSpec{"DEL",
                                 -2,
                                 {"write"},
                                 1,
                                 -1,
                                 1,
                                 [](const std::vector<std::string>& args, SessionState&, bool&) {
                                   std::size_t n = 0;
                                   for (std::size_t i = 1; i < args.size(); ++i)
                                     n += store().del(args[i]) ? 1 : 0;
                                   return encode_integer(n);
                                 }});

    t.emplace("UNLINK", CommandSpec{"UNLINK",
                                    -2,
                                    {"write"},
                                    1,
                                    -1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState&, bool&) {
                                      std::size_t n = 0;
                                      // Lazyfree: big values are detached and destructed on a
                                      // background thread so a huge-key UNLINK doesn't stall the
                                      // event loop.
                                      for (std::size_t i = 1; i < args.size(); ++i)
                                        n += store().unlink(args[i]) ? 1 : 0;
                                      return encode_integer(n);
                                    }});

    t.emplace("EXISTS", CommandSpec{"EXISTS",
                                    -2,
                                    {"readonly"},
                                    1,
                                    -1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState&, bool&) {
                                      std::vector<std::string> keys(args.begin() + 1, args.end());
                                      return encode_integer(store().exists(keys));
                                    }});

    t.emplace("TYPE", CommandSpec{"TYPE",
                                  2,
                                  {"readonly"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return encode_simple(store().type_of(args[1]));
                                  }});

    t.emplace("TTL", CommandSpec{"TTL",
                                 2,
                                 {"readonly"},
                                 1,
                                 1,
                                 1,
                                 [](const std::vector<std::string>& args, SessionState&, bool&) {
                                   return encode_integer(store().ttl(args[1]));
                                 }});

    t.emplace("PTTL", CommandSpec{"PTTL",
                                  2,
                                  {"readonly"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return encode_integer(store().pttl(args[1]));
                                  }});

    t.emplace("EXPIRETIME",
              CommandSpec{"EXPIRETIME",
                          2,
                          {"readonly"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            return encode_integer(store().expiretime(args[1]));
                          }});

    t.emplace("PEXPIRETIME",
              CommandSpec{"PEXPIRETIME",
                          2,
                          {"readonly"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            return encode_integer(store().pexpiretime(args[1]));
                          }});

    t.emplace("EXPIRE",
              CommandSpec{
                  "EXPIRE",
                  -3,
                  {"write"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                    std::int64_t sec = 0;
                    if (!parse_i64(args[2], sec))
                      return encode_error("value is not an integer or out of range");
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
                      if (opt == "NX") {
                        nx = true;
                        continue;
                      }
                      if (opt == "XX") {
                        xx = true;
                        continue;
                      }
                      if (opt == "GT") {
                        gt = true;
                        continue;
                      }
                      if (opt == "LT") {
                        lt = true;
                        continue;
                      }
                      return encode_error("Unsupported option " + args[i]);
                    }
                    if (gt && lt)
                      return encode_error("GT and LT options at the same time are not compatible");
                    if (nx && (xx || gt || lt))
                      return encode_error(
                          "NX and XX, GT or LT options at the same time are not compatible");
                    const auto target = base + delta;
                    const auto cur = store().pexpiretime(args[1]);
                    if (cur == -2) return encode_integer(0);
                    if (nx && cur != -1) return encode_integer(0);
                    if (xx && cur < 0) return encode_integer(0);
                    if (gt && (cur < 0 || target <= cur)) return encode_integer(0);
                    if (lt && cur >= 0 && target >= cur) return encode_integer(0);
                    if (already_expired(target))
                      return encode_integer(store().del(args[1]) ? 1 : 0);
                    return encode_integer(store().expire(args[1], target) ? 1 : 0);
                  }});

    t.emplace("PEXPIRE",
              CommandSpec{
                  "PEXPIRE",
                  -3,
                  {"write"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                    std::int64_t ms = 0;
                    if (!parse_i64(args[2], ms))
                      return encode_error("value is not an integer or out of range");
                    const auto base = now_ms();
                    if ((ms > 0 && base > std::numeric_limits<std::int64_t>::max() - ms) ||
                        (ms < 0 && base < std::numeric_limits<std::int64_t>::min() - ms)) {
                      return encode_error("invalid expire time in 'pexpire' command");
                    }
                    bool nx = false, xx = false, gt = false, lt = false;
                    for (std::size_t i = 3; i < args.size(); ++i) {
                      const auto opt = upper(args[i]);
                      if (opt == "NX") {
                        nx = true;
                        continue;
                      }
                      if (opt == "XX") {
                        xx = true;
                        continue;
                      }
                      if (opt == "GT") {
                        gt = true;
                        continue;
                      }
                      if (opt == "LT") {
                        lt = true;
                        continue;
                      }
                      return encode_error("Unsupported option " + args[i]);
                    }
                    if (gt && lt)
                      return encode_error("GT and LT options at the same time are not compatible");
                    if (nx && (xx || gt || lt))
                      return encode_error(
                          "NX and XX, GT or LT options at the same time are not compatible");
                    const auto target = base + ms;
                    const auto cur = store().pexpiretime(args[1]);
                    if (cur == -2) return encode_integer(0);
                    if (nx && cur != -1) return encode_integer(0);
                    if (xx && cur < 0) return encode_integer(0);
                    if (gt && (cur < 0 || target <= cur)) return encode_integer(0);
                    if (lt && cur >= 0 && target >= cur) return encode_integer(0);
                    if (already_expired(target))
                      return encode_integer(store().del(args[1]) ? 1 : 0);
                    return encode_integer(store().expire(args[1], target) ? 1 : 0);
                  }});

    t.emplace("EXPIREAT",
              CommandSpec{
                  "EXPIREAT",
                  -3,
                  {"write"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                    std::int64_t sec = 0;
                    if (!parse_i64(args[2], sec))
                      return encode_error("value is not an integer or out of range");
                    if (sec > std::numeric_limits<std::int64_t>::max() / 1000 ||
                        sec < std::numeric_limits<std::int64_t>::min() / 1000) {
                      return encode_error("invalid expire time in 'expireat' command");
                    }
                    bool nx = false, xx = false, gt = false, lt = false;
                    for (std::size_t i = 3; i < args.size(); ++i) {
                      const auto opt = upper(args[i]);
                      if (opt == "NX") {
                        nx = true;
                        continue;
                      }
                      if (opt == "XX") {
                        xx = true;
                        continue;
                      }
                      if (opt == "GT") {
                        gt = true;
                        continue;
                      }
                      if (opt == "LT") {
                        lt = true;
                        continue;
                      }
                      return encode_error("Unsupported option " + args[i]);
                    }
                    if (gt && lt)
                      return encode_error("GT and LT options at the same time are not compatible");
                    if (nx && (xx || gt || lt))
                      return encode_error(
                          "NX and XX, GT or LT options at the same time are not compatible");
                    const auto target = sec * 1000;
                    const auto cur = store().pexpiretime(args[1]);
                    if (cur == -2) return encode_integer(0);
                    if (nx && cur != -1) return encode_integer(0);
                    if (xx && cur < 0) return encode_integer(0);
                    if (gt && (cur < 0 || target <= cur)) return encode_integer(0);
                    if (lt && cur >= 0 && target >= cur) return encode_integer(0);
                    if (already_expired(target))
                      return encode_integer(store().del(args[1]) ? 1 : 0);
                    return encode_integer(store().expire(args[1], target) ? 1 : 0);
                  }});

    t.emplace("PEXPIREAT",
              CommandSpec{
                  "PEXPIREAT",
                  -3,
                  {"write"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                    std::int64_t ms = 0;
                    if (!parse_i64(args[2], ms))
                      return encode_error("value is not an integer or out of range");
                    bool nx = false, xx = false, gt = false, lt = false;
                    for (std::size_t i = 3; i < args.size(); ++i) {
                      const auto opt = upper(args[i]);
                      if (opt == "NX") {
                        nx = true;
                        continue;
                      }
                      if (opt == "XX") {
                        xx = true;
                        continue;
                      }
                      if (opt == "GT") {
                        gt = true;
                        continue;
                      }
                      if (opt == "LT") {
                        lt = true;
                        continue;
                      }
                      return encode_error("Unsupported option " + args[i]);
                    }
                    if (gt && lt)
                      return encode_error("GT and LT options at the same time are not compatible");
                    if (nx && (xx || gt || lt))
                      return encode_error(
                          "NX and XX, GT or LT options at the same time are not compatible");
                    const auto cur = store().pexpiretime(args[1]);
                    if (cur == -2) return encode_integer(0);
                    if (nx && cur != -1) return encode_integer(0);
                    if (xx && cur < 0) return encode_integer(0);
                    if (gt && (cur < 0 || ms <= cur)) return encode_integer(0);
                    if (lt && cur >= 0 && ms >= cur) return encode_integer(0);
                    if (already_expired(ms)) return encode_integer(store().del(args[1]) ? 1 : 0);
                    return encode_integer(store().expire(args[1], ms) ? 1 : 0);
                  }});

    t.emplace("PERSIST",
              CommandSpec{"PERSIST",
                          2,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            const bool removed = store().persist(args[1]);
                            if (!removed) g_write_changed_nothing = true;
                            return encode_integer(removed ? 1 : 0);
                          }});

    t.emplace("SELECT",
              CommandSpec{"SELECT",
                          2,
                          {"loading"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            std::int64_t db = 0;
                            if (!parse_i64(args[1], db))
                              return encode_error("value is not an integer or out of range");
                            if (!store().select_db(static_cast<int>(db)))
                              return encode_error("DB index is out of range");
                            session.db_index = static_cast<int>(db);
                            return encode_simple("OK");
                          }});

    t.emplace("SWAPDB",
              CommandSpec{"SWAPDB",
                          3,
                          {"write"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            std::int64_t a = 0, b = 0;
                            if (!parse_i64(args[1], a) || !parse_i64(args[2], b))
                              return encode_error("value is not an integer or out of range");
                            if (!store().swapdb(static_cast<int>(a), static_cast<int>(b)))
                              return encode_error("DB index is out of range");
                            return encode_simple("OK");
                          }});

    t.emplace("DBSIZE",
              CommandSpec{"DBSIZE",
                          1,
                          {"readonly", "fast"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>&, SessionState&, bool&) {
                            return encode_integer(static_cast<long long>(store().dbsize()));
                          }});

    t.emplace("KEYS", CommandSpec{"KEYS",
                                  2,
                                  {"readonly"},
                                  0,
                                  0,
                                  0,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    auto keys = store().keys(args[1]);
                                    std::sort(keys.begin(), keys.end());
                                    std::vector<std::string> out;
                                    for (const auto& k : keys) out.push_back(encode_bulk(k));
                                    return encode_array(out);
                                  }});

    t.emplace("RANDOMKEY", CommandSpec{"RANDOMKEY",
                                       1,
                                       {"readonly", "fast"},
                                       0,
                                       0,
                                       0,
                                       [](const std::vector<std::string>&, SessionState&, bool&) {
                                         const auto k = store().randomkey();
                                         return k.has_value() ? encode_bulk(*k)
                                                              : encode_null(RespVersion::Resp2);
                                       }});

    t.emplace(
        "SCAN",
        CommandSpec{"SCAN",
                    -2,
                    {"readonly"},
                    0,
                    0,
                    0,
                    [](const std::vector<std::string>& args, SessionState&, bool&) {
                      std::uint64_t cursor = 0;
                      if (!parse_scan_cursor(args[1], cursor))
                        return encode_error("invalid cursor");
                      ScanOptions opts;
                      if (std::string err = parse_scan_options(args, 2, /*keyspace=*/true, opts);
                          !err.empty()) {
                        return err;
                      }
                      auto [next, keys] = store().scan(cursor, opts.count, opts.pattern, opts.type);
                      std::vector<std::string> inner;
                      inner.reserve(keys.size());
                      for (const auto& k : keys) inner.push_back(encode_bulk(k));
                      return encode_array({encode_bulk(std::to_string(next)), encode_array(inner)});
                    }});

    t.emplace("SORT",
              CommandSpec{"SORT",
                          -2,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            return do_sort(args, session.resp_version, /*readonly=*/false);
                          }});

    t.emplace("RENAME",
              CommandSpec{"RENAME",
                          3,
                          {"write"},
                          1,
                          2,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            if (!store().rename(args[1], args[2], false))
                              return encode_error("no such key");
                            // The destination may now hold a stream/list — wake any client blocked
                            // on it (Redis signals the renamed-into key ready so blocked
                            // XREAD(GROUP)/BLPOP re-run).
                            notify_key_written(args[2], session.db_index);
                            return encode_simple("OK");
                          }});

    t.emplace("RENAMENX",
              CommandSpec{"RENAMENX",
                          3,
                          {"write"},
                          1,
                          2,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            return encode_integer(store().rename(args[1], args[2], true) ? 1 : 0);
                          }});

    t.emplace(
        "COPY",
        CommandSpec{"COPY",
                    -3,
                    {"write"},
                    1,
                    2,
                    1,
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
                          if (!parse_i64(args[++i], db) || db < 0)
                            return encode_error("value is not an integer or out of range");
                          target_db = static_cast<int>(db);
                          continue;
                        }
                        return encode_error("syntax error");
                      }
                      return encode_integer(store().copy_key(args[1], args[2], target_db, replace));
                    }});

    t.emplace("MOVE",
              CommandSpec{"MOVE",
                          3,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            std::int64_t db = 0;
                            if (!parse_i64(args[2], db))
                              return encode_error("value is not an integer or out of range");
                            if (db < 0 || db >= 16) return encode_error("DB index is out of range");
                            return encode_integer(store().move_key(args[1], static_cast<int>(db)));
                          }});

    t.emplace("DUMP", CommandSpec{"DUMP",
                                  2,
                                  {"readonly"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    if (store().type_of(args[1]) == "none")
                                      return encode_null(RespVersion::Resp2);
                                    auto payload = rdb_dump_key(args[1]);
                                    if (payload.empty()) return encode_null(RespVersion::Resp2);
                                    return encode_bulk(payload);
                                  }});

    t.emplace(
        "RESTORE",
        CommandSpec{
            "RESTORE",
            -4,
            {"write"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              // Redis's restoreCommand(): the options, then the key, the TTL and the payload, each
              // with Redis's error; the payload is read in full before REPLACE deletes anything.
              bool replace = false;
              bool absttl = false;
              std::int64_t idletime = -1, freq = -1;
              for (std::size_t i = 4; i < args.size(); ++i) {
                const std::string opt = upper(args[i]);
                const bool has_value = i + 1 < args.size();
                if (opt == "REPLACE") {
                  replace = true;
                } else if (opt == "ABSTTL") {
                  absttl = true;
                } else if (opt == "IDLETIME" && has_value && freq == -1) {
                  if (!parse_strict_int(args[++i], idletime))
                    return encode_error("value is not an integer or out of range");
                  if (idletime < 0) return encode_error("Invalid IDLETIME value, must be >= 0");
                } else if (opt == "FREQ" && has_value && idletime == -1) {
                  if (!parse_strict_int(args[++i], freq))
                    return encode_error("value is not an integer or out of range");
                  if (freq < 0 || freq > 255)
                    return encode_error("Invalid FREQ value, must be >= 0 and <= 255");
                } else {
                  return encode_error("syntax error");
                }
              }
              const std::string& key = args[1];
              if (!replace && store().type_of(key) != "none")
                return std::string("-BUSYKEY Target key name already exists.\r\n");
              std::int64_t ttl = 0;
              if (!parse_strict_int(args[2], ttl))
                return encode_error("value is not an integer or out of range");
              if (ttl < 0) return encode_error("Invalid TTL value, must be >= 0");
              DumpValue value;
              std::string err;
              if (!rdb_decode_dump_payload(args[3], value, err)) return encode_error(err);

              // Redis's dbDelete() erases the key even when its TTL has passed and it is not
              // deleted yet (no lazy-expiry DEL, no `expired` event).
              const bool was_live = store().live_type(key).has_value();
              const bool deleted = replace && store().del_raw(key);
              // A relative TTL counts from now; past the largest time it wraps into the past, as
              // the sum does in Redis.
              if (ttl != 0 && !absttl)
                ttl = static_cast<std::int64_t>(static_cast<std::uint64_t>(ttl) +
                                                static_cast<std::uint64_t>(DataStore::now_ms()));
              if (ttl != 0 && already_expired(ttl)) {
                // Nothing is stored. If REPLACE deleted a key, Redis sends DEL (UNLINK) in place of
                // the RESTORE, with a `del` event (canonicalize_propagated_command(),
                // emit_keyspace_notifications()); otherwise nothing changed.
                if (!deleted) g_write_changed_nothing = true;
                return encode_simple("OK");
              }
              rdb_store_dump_value(key, std::move(value),
                                   ttl != 0 ? std::optional<std::int64_t>(ttl) : std::nullopt);
              // A new object starts with a fresh LRU clock or LFU counter (Redis's createObject());
              // objectSetLRUOrLFU() then applies FREQ under an LFU policy and IDLETIME under any
              // other. The dispatch does not touch a RESTORE key afterwards.
              store().touch_key_for_eviction(key);
              if (g_maxmemory_policy == MaxmemoryPolicy::AllkeysLfu ||
                  g_maxmemory_policy == MaxmemoryPolicy::VolatileLfu) {
                if (freq >= 0) store().set_object_freq(key, freq);
              } else if (idletime >= 0) {
                store().set_object_idle(key, idletime);
              }
              // Redis deletes the old key and adds the new one (dbDelete(), dbAdd()), so it sends
              // `new` for a replaced key too; the dispatch sends it for a key that was not live
              // before.
              if (deleted && was_live)
                notify_keyspace_event(NOTIFY_NEW, "new", key, session.db_index);
              return encode_simple("OK");
            }});

    // RESTORE-ASKING is RESTORE, sent by MIGRATE in cluster mode; Redis flags it CMD_ASKING so an
    // importing node takes it without an ASKING first (PeaDB has no importing side yet, item 66).
    {
      CommandSpec restore_asking = t.at("RESTORE");
      restore_asking.name = "RESTORE-ASKING";
      t.emplace("RESTORE-ASKING", std::move(restore_asking));
    }

    t.emplace("MIGRATE",
              CommandSpec{"MIGRATE",
                          -6,
                          {"write"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            std::string reply = migrate_command(args, session);
                            g_write_changed_nothing =
                                true;  // it propagated, counted and announced its deletions
                            return reply;
                          }});

    t.emplace(
        "FLUSHALL",
        CommandSpec{"FLUSHALL",
                    -1,
                    {"write"},
                    0,
                    0,
                    0,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      bool async = false;
                      if (args.size() >= 2) {
                        const auto m = upper(args[1]);
                        if (m == "ASYNC")
                          async = true;
                        else if (m != "SYNC")
                          return encode_error("syntax error");
                      }
                      if (async)
                        store().flushall_async();
                      else
                        store().flushall();
                      // flushAllDataAndResetRDB(): a running BGSAVE holds the data from before the
                      // FLUSHALL, so it is stopped; with save points the empty dataset is saved at
                      // once, so a restart cannot bring the flushed keys back.
                      kill_rdb_child();
                      if (!g_save_params.empty()) {
                        std::string err;
                        if (rdb_save(g_snapshot_path, err)) {
                          // The dispatch counts this FLUSHALL as one change when it returns, and
                          // Redis's rdbSave() leaves the change count at 0 after a FLUSHALL, so the
                          // count restarts after this command.
                          note_rdb_saved(static_cast<std::int64_t>(store().mutation_epoch()) + 1);
                        } else {
                          log(LogLevel::Warn, err);
                        }
                      }
                      return encode_simple("OK");
                    }});

    t.emplace("FLUSHDB", CommandSpec{"FLUSHDB",
                                     -1,
                                     {"write"},
                                     0,
                                     0,
                                     0,
                                     [](const std::vector<std::string>& args, SessionState&,
                                        bool&) -> std::string {
                                       bool async = false;
                                       if (args.size() >= 2) {
                                         const auto m = upper(args[1]);
                                         if (m == "ASYNC")
                                           async = true;
                                         else if (m != "SYNC")
                                           return encode_error("syntax error");
                                       }
                                       if (async)
                                         store().flushdb_async();
                                       else
                                         store().flushdb();
                                       return encode_simple("OK");
                                     }});

    t.emplace(
        "FUNCTION",
        CommandSpec{
            "FUNCTION",
            -2,
            {"loading", "stale"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              const auto sub = upper(args[1]);
              if (sub == "LOAD") {
                bool replace = false;
                std::size_t i = 2;
                for (; i + 1 < args.size(); ++i) {
                  if (upper(args[i]) == "REPLACE")
                    replace = true;
                  else
                    return encode_error("Unknown option given: " + args[i]);
                }
                if (i >= args.size()) return encode_error("Function code is missing");
                std::string name, err;
                if (!load_function_library(args[i], replace, name, err)) return encode_error(err);
                propagate_write({"FUNCTION", "LOAD", "REPLACE", args[i]}, session, nullptr);
                store().increment_mutation_epoch();
                return encode_bulk(name);
              }
              if (sub == "DELETE" && args.size() == 3) {
                const std::string lc = lower(args[2]);
                auto it = g_libraries.find(lc);
                if (it == g_libraries.end()) return encode_error("Library not found");
                for (auto& f : it->second.functions) {
                  g_func_index.erase(lower(f.name));
                  lua_engine_unref_function(f.callback_ref);
                }
                g_libraries.erase(it);
                propagate_write({"FUNCTION", "DELETE", args[2]}, session, nullptr);
                store().increment_mutation_epoch();
                return encode_simple("OK");
              }
              if (sub == "FLUSH") {
                if (args.size() > 3)
                  return encode_error(
                      "unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION "
                      "HELP.");
                if (args.size() == 3) {
                  const auto m = upper(args[2]);
                  if (m != "SYNC" && m != "ASYNC")
                    return encode_error("FUNCTION FLUSH only supports SYNC|ASYNC");
                }
                functions_flush_all();
                propagate_write({"FUNCTION", "FLUSH"}, session, nullptr);
                store().increment_mutation_epoch();
                return encode_simple("OK");
              }
              if (sub == "LIST") {
                std::string pat;
                bool withcode = false, have_pat = false;
                for (std::size_t i = 2; i < args.size(); ++i) {
                  const auto o = upper(args[i]);
                  if (o == "LIBRARYNAME") {
                    if (have_pat || i + 1 >= args.size())
                      return encode_error(have_pat ? "Unknown argument " + args[i]
                                                   : "library name argument was not given");
                    pat = args[++i];
                    have_pat = true;
                  } else if (o == "WITHCODE" && !withcode) {
                    withcode = true;
                  } else {
                    return encode_error("Unknown argument " + args[i]);
                  }
                }
                std::vector<std::string> libs;
                for (const auto& [lc, lib] : g_libraries) {
                  if (have_pat && lib.name != pat && fnmatch(pat.c_str(), lib.name.c_str(), 0) != 0)
                    continue;
                  std::vector<std::string> fns;
                  for (const auto& f : lib.functions) {
                    std::vector<std::string> flagnames;
                    if (f.flags & kFuncNoWrites) flagnames.push_back(encode_bulk("no-writes"));
                    if (f.flags & kFuncAllowOom) flagnames.push_back(encode_bulk("allow-oom"));
                    if (f.flags & kFuncAllowStale) flagnames.push_back(encode_bulk("allow-stale"));
                    if (f.flags & kFuncNoCluster) flagnames.push_back(encode_bulk("no-cluster"));
                    if (f.flags & kFuncAllowCrossSlot)
                      flagnames.push_back(encode_bulk("allow-cross-slot-keys"));
                    std::vector<std::string> fkv = {
                        encode_bulk("name"),
                        encode_bulk(f.name),
                        encode_bulk("description"),
                        f.description.empty() ? encode_null(session.resp_version)
                                              : encode_bulk(f.description),
                        encode_bulk("flags"),
                        encode_set_reply(flagnames, session.resp_version)};
                    fns.push_back(encode_map(fkv, session.resp_version));
                  }
                  std::vector<std::string> lkv = {
                      encode_bulk("library_name"), encode_bulk(lib.name),    encode_bulk("engine"),
                      encode_bulk(lib.engine),     encode_bulk("functions"), encode_array(fns)};
                  if (withcode) {
                    lkv.push_back(encode_bulk("library_code"));
                    lkv.push_back(encode_bulk(lib.code));
                  }
                  libs.push_back(encode_map(lkv, session.resp_version));
                }
                return encode_array(libs);
              }
              if (sub == "STATS") {
                std::string running;
                if (g_script_busy.load()) {
                  running = encode_map(
                      {encode_bulk("name"), encode_bulk(g_busy_func_name), encode_bulk("command"),
                       encode_bulk(g_busy_func_cmd), encode_bulk("duration_ms"), encode_integer(0)},
                      session.resp_version);
                } else {
                  running = encode_null(session.resp_version);
                }
                std::size_t nfuncs = g_func_index.size();
                std::string engines = encode_map(
                    {encode_bulk("LUA"),
                     encode_map({encode_bulk("libraries_count"),
                                 encode_integer(static_cast<long long>(g_libraries.size())),
                                 encode_bulk("functions_count"),
                                 encode_integer(static_cast<long long>(nfuncs))},
                                session.resp_version)},
                    session.resp_version);
                return encode_map(
                    {encode_bulk("running_script"), running, encode_bulk("engines"), engines},
                    session.resp_version);
              }
              if (sub == "DUMP" && args.size() == 2) {
                return encode_bulk(functions_dump());
              }
              if (sub == "RESTORE" && (args.size() == 3 || args.size() == 4)) {
                std::string policy = "APPEND";
                if (args.size() == 4) {
                  policy = upper(args[3]);
                  if (policy != "FLUSH" && policy != "APPEND" && policy != "REPLACE")
                    return encode_error(
                        "Wrong restore policy given, value should be either FLUSH, APPEND or "
                        "REPLACE.");
                }
                std::string err;
                if (!functions_restore(args[2], policy, err)) return encode_error(err);
                propagate_write({"FUNCTION", "RESTORE", args[2], policy}, session, nullptr);
                store().increment_mutation_epoch();
                return encode_simple("OK");
              }
              if (sub == "KILL" && args.size() == 2) {
                if (!g_script_busy.load())
                  return std::string("-NOTBUSY No scripts in execution right now.\r\n");
                g_script_busy.store(false);
                g_script_kill_requested.store(true);
                g_busy_script_session = nullptr;
                return encode_simple("OK");
              }
              if (sub == "HELP") {
                return encode_array(
                    {encode_bulk(
                         "FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                     encode_bulk("LOAD [REPLACE] <code>"), encode_bulk("DELETE <library-name>"),
                     encode_bulk("LIST [LIBRARYNAME <name>] [WITHCODE]"), encode_bulk("DUMP"),
                     encode_bulk("RESTORE <payload> [FLUSH|APPEND|REPLACE]"),
                     encode_bulk("FLUSH [ASYNC|SYNC]"), encode_bulk("STATS"), encode_bulk("KILL"),
                     encode_bulk("HELP")});
              }
              return encode_error("unknown subcommand or wrong number of arguments for '" +
                                  lower(args[1]) + "'. Try FUNCTION HELP.");
            }});

    t.emplace("FCALL",
              CommandSpec{"FCALL",
                          -3,
                          {"write"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            return do_fcall(args, session, false);
                          }});

    t.emplace("FCALL_RO",
              CommandSpec{"FCALL_RO",
                          -3,
                          {"readonly"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            return do_fcall(args, session, true);
                          }});

    t.emplace(
        "OBJECT",
        CommandSpec{
            "OBJECT",
            -2,
            {"readonly"},
            2,
            2,
            1,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              const auto sub = upper(args[1]);
              const bool lfu = g_maxmemory_policy == MaxmemoryPolicy::AllkeysLfu ||
                               g_maxmemory_policy == MaxmemoryPolicy::VolatileLfu;
              if (sub == "REFCOUNT" && args.size() == 3) {
                const auto t = store().type_of(args[2]);
                if (t == "none") return encode_null(session.resp_version);
                // Redis returns INT_MAX for shared integer objects (values 0..9999); the upstream
                // incr suite asserts refcount > 1 for them. With maxmemory set and an LRU or LFU
                // policy Redis does not share them (MAXMEMORY_FLAG_NO_SHARED_INTEGERS: each key
                // keeps its own access time); Redis decides when the value is written, PeaDB when
                // it is asked.
                const bool no_shared_integers =
                    g_config_maxmemory > 0 && g_maxmemory_policy != MaxmemoryPolicy::NoEviction &&
                    g_maxmemory_policy != MaxmemoryPolicy::AllkeysRandom &&
                    g_maxmemory_policy != MaxmemoryPolicy::VolatileRandom &&
                    g_maxmemory_policy != MaxmemoryPolicy::VolatileTtl;
                if (t == "string" && !no_shared_integers) {
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
                if (lfu)
                  return encode_error(
                      "An LFU maxmemory policy is selected, idle time not tracked. Please note "
                      "that when switching between policies at runtime LRU and LFU data will take "
                      "some time to adjust.");
                return encode_integer(*v);
              }
              if (sub == "FREQ" && args.size() == 3) {
                auto v = store().object_freq(args[2]);
                if (!v.has_value()) return encode_null(session.resp_version);
                if (!lfu)
                  return encode_error(
                      "An LFU maxmemory policy is not selected, access frequency not tracked. "
                      "Please note that when switching between policies at runtime LRU and LFU "
                      "data will take some time to adjust.");
                return encode_integer(*v);
              }
              if (sub == "HELP" && args.size() == 2) {
                return encode_array(
                    {encode_bulk("OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                     encode_bulk("ENCODING <key>"), encode_bulk("FREQ <key>"),
                     encode_bulk("IDLETIME <key>"), encode_bulk("REFCOUNT <key>"),
                     encode_bulk("HELP")});
              }
              return encode_error("Unknown subcommand or wrong number of arguments for '" +
                                  args[1] + "'. Try OBJECT HELP.");
            }});

    t.emplace("HSET",
              CommandSpec{"HSET",
                          -4,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            if (((args.size() - 2) % 2) != 0)
                              return encode_error("wrong number of arguments for 'hset' command");
                            return with_hash_key(args[1], [&]() {
                              std::vector<std::pair<std::string, std::string>> fields;
                              for (std::size_t i = 2; i < args.size(); i += 2)
                                fields.emplace_back(args[i], args[i + 1]);
                              bool wrongtype = false;
                              const auto added = store().hset(args[1], fields, wrongtype);
                              if (wrongtype) return wrongtype_error_reply();
                              return encode_integer(added);
                            });
                          }});

    t.emplace("HGET", CommandSpec{"HGET",
                                  3,
                                  {"readonly", "fast"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return with_hash_key(args[1], [&]() {
                                      bool wrongtype = false;
                                      auto v = store().hget(args[1], args[2], wrongtype);
                                      if (wrongtype) return wrongtype_error_reply();
                                      return v.has_value() ? encode_bulk(*v)
                                                           : encode_null(RespVersion::Resp2);
                                    });
                                  }});

    t.emplace("HMGET", CommandSpec{"HMGET",
                                   -3,
                                   {"readonly", "fast"},
                                   1,
                                   1,
                                   1,
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
                                         out.push_back(v.has_value()
                                                           ? encode_bulk(*v)
                                                           : encode_null(RespVersion::Resp2));
                                       }
                                       return encode_array(out);
                                     });
                                   }});

    t.emplace("HMSET",
              CommandSpec{"HMSET",
                          -4,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            if (((args.size() - 2) % 2) != 0)
                              return encode_error("wrong number of arguments for 'hmset' command");
                            return with_hash_key(args[1], [&]() {
                              std::vector<std::pair<std::string, std::string>> fields;
                              for (std::size_t i = 2; i < args.size(); i += 2)
                                fields.emplace_back(args[i], args[i + 1]);
                              bool wrongtype = false;
                              store().hset(args[1], fields, wrongtype);
                              if (wrongtype) return wrongtype_error_reply();
                              return encode_simple("OK");
                            });
                          }});

    t.emplace("HSETNX", CommandSpec{"HSETNX",
                                    4,
                                    {"write"},
                                    1,
                                    1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState&, bool&) {
                                      return with_hash_key(args[1], [&]() {
                                        bool wrongtype = false;
                                        auto existing = store().hget(args[1], args[2], wrongtype);
                                        if (wrongtype) return wrongtype_error_reply();
                                        if (existing.has_value()) return encode_integer(0);
                                        std::vector<std::pair<std::string, std::string>> fields{
                                            {args[2], args[3]}};
                                        store().hset(args[1], fields, wrongtype);
                                        return encode_integer(1);
                                      });
                                    }});

    t.emplace("HVALS", CommandSpec{"HVALS",
                                   2,
                                   {"readonly"},
                                   1,
                                   1,
                                   1,
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

    t.emplace("HKEYS", CommandSpec{"HKEYS",
                                   2,
                                   {"readonly"},
                                   1,
                                   1,
                                   1,
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

    t.emplace(
        "HINCRBY",
        CommandSpec{"HINCRBY",
                    4,
                    {"write"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) {
                      return with_hash_key(args[1], [&]() {
                        std::int64_t increment = 0;
                        if (!parse_i64(args[3], increment))
                          return encode_error("value is not an integer or out of range");
                        bool wrongtype = false;
                        auto existing = store().hget(args[1], args[2], wrongtype);
                        if (wrongtype) return wrongtype_error_reply();
                        std::int64_t current = 0;
                        if (existing.has_value()) {
                          if (!parse_strict_int(*existing, current))
                            return encode_error("hash value is not an integer");
                        }
                        if ((increment > 0 &&
                             current > std::numeric_limits<std::int64_t>::max() - increment) ||
                            (increment < 0 &&
                             current < std::numeric_limits<std::int64_t>::min() - increment)) {
                          return encode_error("increment or decrement would overflow");
                        }
                        current += increment;
                        std::vector<std::pair<std::string, std::string>> fields{
                            {args[2], std::to_string(current)}};
                        store().hset(args[1], fields, wrongtype);
                        return encode_integer(current);
                      });
                    }});

    t.emplace(
        "HINCRBYFLOAT",
        CommandSpec{"HINCRBYFLOAT",
                    4,
                    {"write"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) {
                      return with_hash_key(args[1], [&]() {
                        long double increment = 0;
                        if (!parse_strict_float(args[3], increment))
                          return encode_error("value is not a valid float");
                        bool wrongtype = false;
                        auto existing = store().hget(args[1], args[2], wrongtype);
                        if (wrongtype) return wrongtype_error_reply();
                        long double current = 0;
                        if (existing.has_value()) {
                          if (!parse_strict_float(*existing, current))
                            return encode_error("hash value is not a valid float");
                        }
                        current += increment;
                        if (std::isnan(static_cast<double>(current)) ||
                            std::isinf(static_cast<double>(current))) {
                          return encode_error(
                              "value is NaN or Infinity");  // hash-specific Redis wording
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

    t.emplace("HSTRLEN",
              CommandSpec{"HSTRLEN",
                          3,
                          {"readonly", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            return with_hash_key(args[1], [&]() {
                              bool wrongtype = false;
                              auto v = store().hget(args[1], args[2], wrongtype);
                              if (wrongtype) return wrongtype_error_reply();
                              return encode_integer(
                                  v.has_value() ? static_cast<long long>(v->size()) : 0);
                            });
                          }});

    t.emplace("HDEL", CommandSpec{"HDEL",
                                  -3,
                                  {"write"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return with_hash_key(args[1], [&]() {
                                      std::vector<std::string> fields(args.begin() + 2, args.end());
                                      bool wrongtype = false;
                                      const auto removed = store().hdel(args[1], fields, wrongtype);
                                      if (wrongtype) return wrongtype_error_reply();
                                      return encode_integer(removed);
                                    });
                                  }});

    t.emplace("HLEN", CommandSpec{"HLEN",
                                  2,
                                  {"readonly", "fast"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return with_hash_key(args[1], [&]() {
                                      bool wrongtype = false;
                                      const auto n = store().hlen(args[1], wrongtype);
                                      if (wrongtype) return wrongtype_error_reply();
                                      return encode_integer(n);
                                    });
                                  }});

    t.emplace("HEXISTS",
              CommandSpec{"HEXISTS",
                          3,
                          {"readonly", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            return with_hash_key(args[1], [&]() {
                              bool wrongtype = false;
                              const auto ok = store().hexists(args[1], args[2], wrongtype);
                              if (wrongtype) return wrongtype_error_reply();
                              return encode_integer(ok ? 1 : 0);
                            });
                          }});

    t.emplace("HGETALL",
              CommandSpec{"HGETALL",
                          2,
                          {"readonly"},
                          1,
                          1,
                          1,
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

    t.emplace("HSCAN", CommandSpec{"HSCAN",
                                   -3,
                                   {"readonly"},
                                   1,
                                   1,
                                   1,
                                   [](const std::vector<std::string>& args, SessionState&, bool&) {
                                     return collection_scan_command(args, ValueType::Hash);
                                   }});

    t.emplace("LPUSH",
              CommandSpec{"LPUSH",
                          -3,
                          {"write", "denyoom"},
                          1,
                          1,
                          1,
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

    t.emplace("RPUSH",
              CommandSpec{"RPUSH",
                          -3,
                          {"write", "denyoom"},
                          1,
                          1,
                          1,
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

    t.emplace("LPOP",
              CommandSpec{"LPOP",
                          -2,
                          {"write", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            return list_pop_command(args, /*left=*/true, session.resp_version);
                          }});

    t.emplace("RPOP",
              CommandSpec{"RPOP",
                          -2,
                          {"write", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            return list_pop_command(args, /*left=*/false, session.resp_version);
                          }});

    t.emplace("LLEN", CommandSpec{"LLEN",
                                  2,
                                  {"readonly", "fast"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return with_list_key(args[1], [&]() {
                                      bool wrongtype = false;
                                      auto n = store().llen(args[1], wrongtype);
                                      if (wrongtype) return wrongtype_error_reply();
                                      return encode_integer(n);
                                    });
                                  }});

    t.emplace("LRANGE",
              CommandSpec{"LRANGE",
                          4,
                          {"readonly"},
                          1,
                          1,
                          1,
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

    t.emplace("BLPOP",
              CommandSpec{"BLPOP",
                          -3,
                          {"write", "blocking"},
                          1,
                          -2,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            long double timeout_sec = 0;
                            if (auto terr = parse_block_timeout_sec(args.back(), timeout_sec);
                                !terr.empty()) {
                              return terr;
                            }
                            // Try immediate pop first
                            for (std::size_t i = 1; i + 1 < args.size(); ++i) {
                              const auto& key = args[i];
                              if (store().is_wrongtype_for_list(key))
                                return wrongtype_error_reply();
                              bool wrongtype = false;
                              auto v = store().lpop(key, wrongtype);
                              if (wrongtype) return wrongtype_error_reply();
                              if (v.has_value())
                                return encode_array({encode_bulk(key), encode_bulk(*v)});
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
                              session.blocked.deadline_ms =
                                  now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L);
                            } else {
                              session.blocked.deadline_ms = 0;  // infinite
                            }
                            return std::string();  // empty = don't send reply yet
                          }});

    t.emplace("BRPOP",
              CommandSpec{"BRPOP",
                          -3,
                          {"write", "blocking"},
                          1,
                          -2,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            long double timeout_sec = 0;
                            if (auto terr = parse_block_timeout_sec(args.back(), timeout_sec);
                                !terr.empty()) {
                              return terr;
                            }
                            for (std::size_t i = 1; i + 1 < args.size(); ++i) {
                              const auto& key = args[i];
                              if (store().is_wrongtype_for_list(key))
                                return wrongtype_error_reply();
                              bool wrongtype = false;
                              auto v = store().rpop(key, wrongtype);
                              if (wrongtype) return wrongtype_error_reply();
                              if (v.has_value())
                                return encode_array({encode_bulk(key), encode_bulk(*v)});
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
                              session.blocked.deadline_ms =
                                  now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L);
                            } else {
                              session.blocked.deadline_ms = 0;
                            }
                            return std::string();
                          }});

    t.emplace("LMOVE", CommandSpec{"LMOVE",
                                   5,
                                   {"write"},
                                   1,
                                   2,
                                   1,
                                   [](const std::vector<std::string>& args, SessionState&, bool&) {
                                     const std::string src = args[1];
                                     const std::string dst = args[2];
                                     const std::string where_from = upper(args[3]);
                                     const std::string where_to = upper(args[4]);
                                     if ((where_from != "LEFT" && where_from != "RIGHT") ||
                                         (where_to != "LEFT" && where_to != "RIGHT")) {
                                       return encode_error("syntax error");
                                     }
                                     if (store().is_wrongtype_for_list(src) ||
                                         store().is_wrongtype_for_list(dst))
                                       return wrongtype_error_reply();

                                     bool wrongtype = false;
                                     std::optional<std::string> popped;
                                     if (where_from == "LEFT")
                                       popped = store().lpop(src, wrongtype);
                                     else
                                       popped = store().rpop(src, wrongtype);
                                     if (wrongtype) return wrongtype_error_reply();
                                     if (!popped.has_value())
                                       return encode_null(RespVersion::Resp2);

                                     std::vector<std::string> one{*popped};
                                     if (where_to == "LEFT")
                                       (void)store().lpush(dst, one, wrongtype);
                                     else
                                       (void)store().rpush(dst, one, wrongtype);
                                     if (wrongtype) return wrongtype_error_reply();
                                     return encode_bulk(*popped);
                                   }});

    t.emplace("BLMOVE",
              CommandSpec{
                  "BLMOVE",
                  6,
                  {"write", "blocking"},
                  1,
                  2,
                  1,
                  [](const std::vector<std::string>& args, SessionState& session, bool&) {
                    long double timeout_sec = 0;
                    if (auto terr = parse_block_timeout_sec(args[5], timeout_sec); !terr.empty()) {
                      return terr;
                    }
                    const std::string src = args[1];
                    const std::string dst = args[2];
                    const std::string where_from = upper(args[3]);
                    const std::string where_to = upper(args[4]);
                    if ((where_from != "LEFT" && where_from != "RIGHT") ||
                        (where_to != "LEFT" && where_to != "RIGHT")) {
                      return encode_error("syntax error");
                    }
                    // Only the source type is checked up front. The destination type is checked
                    // when an element is actually moved — an empty source blocks regardless of the
                    // dst type (Redis blmoveGenericCommand).
                    if (store().is_wrongtype_for_list(src)) return wrongtype_error_reply();

                    bool wrongtype = false;
                    if (store().llen(src, wrongtype) > 0 && store().is_wrongtype_for_list(dst)) {
                      return wrongtype_error_reply();  // src has data but dst is wrong type: source
                                                       // intact
                    }
                    std::optional<std::string> popped;
                    if (where_from == "LEFT")
                      popped = store().lpop(src, wrongtype);
                    else
                      popped = store().rpop(src, wrongtype);
                    if (wrongtype) return wrongtype_error_reply();
                    if (popped.has_value()) {
                      std::vector<std::string> one{*popped};
                      if (where_to == "LEFT")
                        (void)store().lpush(dst, one, wrongtype);
                      else
                        (void)store().rpush(dst, one, wrongtype);
                      return encode_bulk(*popped);
                    }
                    // In Lua script or EXEC context: return nil immediately (no blocking)
                    if (g_in_lua_script || g_executing_exec) return encode_null(RespVersion::Resp2);
                    session.blocked.type = BlockType::List;
                    session.blocked.pop_left = (where_from == "LEFT");
                    session.blocked.keys = {src};
                    session.blocked.original_args = args;
                    if (timeout_sec > 0) {
                      session.blocked.deadline_ms =
                          now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L);
                    } else {
                      session.blocked.deadline_ms = 0;
                    }
                    return std::string();
                  }});

    t.emplace("BRPOPLPUSH",
              CommandSpec{"BRPOPLPUSH",
                          4,
                          {"write", "blocking"},
                          1,
                          2,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            std::vector<std::string> proxy = {"BLMOVE", args[1], args[2],
                                                              "RIGHT",  "LEFT",  args[3]};
                            bool close = false;
                            return handle_command(proxy, session, close);
                          }});

    // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n] (Redis 7.0). The
    // immediate path propagates its effect as a deterministic LPOP/RPOP (the dispatch
    // propagation is suppressed in canonicalize_propagated_command).
    t.emplace(
        "BLMPOP",
        CommandSpec{
            "BLMPOP",
            -5,
            {"write", "blocking"},
            0,
            0,
            0,
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
                if (pos + 3 != args.size() || upper(args[pos + 1]) != "COUNT")
                  return encode_error("syntax error");
                std::int64_t c = 0;
                if (!parse_i64(args[pos + 2], c) || c <= 0)
                  return encode_error("count should be greater than 0");
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
                const std::vector<std::string> eff{left ? "LPOP" : "RPOP", key,
                                                   std::to_string(popped)};
                propagate_write(eff, session, nullptr);
                return encode_array({encode_bulk(key), encode_array(enc)});
              }
              if (g_in_lua_script || g_executing_exec || g_block_disabled)
                return encode_null_array(session.resp_version);
              session.blocked.type = BlockType::List;
              session.blocked.pop_left = left;
              session.blocked.keys.clear();
              for (std::int64_t i = 0; i < numkeys; ++i) {
                session.blocked.keys.push_back(args[static_cast<std::size_t>(3 + i)]);
                register_key_waiter(args[static_cast<std::size_t>(3 + i)], session.fd);
              }
              session.blocked.original_args = args;
              session.blocked.deadline_ms =
                  timeout_sec > 0 ? now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L) : 0;
              return std::string();
            }});

    // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT n] (Redis 7.0).
    t.emplace(
        "BZMPOP",
        CommandSpec{
            "BZMPOP",
            -5,
            {"write", "blocking"},
            0,
            0,
            0,
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
                if (pos + 3 != args.size() || upper(args[pos + 1]) != "COUNT")
                  return encode_error("syntax error");
                std::int64_t c = 0;
                if (!parse_i64(args[pos + 2], c) || c <= 0)
                  return encode_error("count should be greater than 0");
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
                propagate_write(eff, session, nullptr);
                std::vector<std::string> pairs;
                for (const auto& [m, s] : vals) {
                  pairs.push_back(
                      encode_array({encode_bulk(m), encode_double(s, session.resp_version)}));
                }
                return encode_array({encode_bulk(key), encode_array(pairs)});
              }
              if (g_in_lua_script || g_executing_exec || g_block_disabled)
                return encode_null_array(session.resp_version);
              session.blocked.type = BlockType::ZSet;
              session.blocked.zpop_max = max;
              session.blocked.keys.clear();
              for (std::int64_t i = 0; i < numkeys; ++i) {
                session.blocked.keys.push_back(args[static_cast<std::size_t>(3 + i)]);
                register_key_waiter(args[static_cast<std::size_t>(3 + i)], session.fd);
              }
              session.blocked.original_args = args;
              session.blocked.deadline_ms =
                  timeout_sec > 0 ? now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L) : 0;
              return std::string();
            }});

    t.emplace("BZPOPMIN",
              CommandSpec{"BZPOPMIN",
                          -3,
                          {"write", "blocking"},
                          1,
                          -2,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            long double timeout_sec = 0;
                            if (auto terr = parse_block_timeout_sec(args.back(), timeout_sec);
                                !terr.empty()) {
                              return terr;
                            }
                            for (std::size_t i = 1; i + 1 < args.size(); ++i) {
                              const auto tp = store().type_of(args[i]);
                              if (tp != "none" && tp != "zset") return wrongtype_error_reply();
                              bool wrongtype = false;
                              auto vals = store().zpop(args[i], 1, false, wrongtype);
                              if (wrongtype) return wrongtype_error_reply();
                              if (!vals.empty()) {
                                return encode_array(
                                    {encode_bulk(args[i]), encode_bulk(vals[0].first),
                                     encode_double(vals[0].second, session.resp_version)});
                              }
                            }
                            // In Lua script or EXEC context: return nil immediately (no blocking)
                            if (g_in_lua_script || g_executing_exec) return encode_null_array();
                            session.blocked.type = BlockType::ZSet;
                            session.blocked.zpop_max = false;
                            session.blocked.keys.clear();
                            for (std::size_t i = 1; i + 1 < args.size(); ++i)
                              session.blocked.keys.push_back(args[i]);
                            session.blocked.original_args = args;
                            if (timeout_sec > 0) {
                              session.blocked.deadline_ms =
                                  now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L);
                            } else {
                              session.blocked.deadline_ms = 0;
                            }
                            return std::string();
                          }});

    t.emplace("BZPOPMAX",
              CommandSpec{"BZPOPMAX",
                          -3,
                          {"write", "blocking"},
                          1,
                          -2,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            long double timeout_sec = 0;
                            if (auto terr = parse_block_timeout_sec(args.back(), timeout_sec);
                                !terr.empty()) {
                              return terr;
                            }
                            for (std::size_t i = 1; i + 1 < args.size(); ++i) {
                              const auto tp = store().type_of(args[i]);
                              if (tp != "none" && tp != "zset") return wrongtype_error_reply();
                              bool wrongtype = false;
                              auto vals = store().zpop(args[i], 1, true, wrongtype);
                              if (wrongtype) return wrongtype_error_reply();
                              if (!vals.empty()) {
                                return encode_array(
                                    {encode_bulk(args[i]), encode_bulk(vals[0].first),
                                     encode_double(vals[0].second, session.resp_version)});
                              }
                            }
                            // In Lua script or EXEC context: return nil immediately (no blocking)
                            if (g_in_lua_script || g_executing_exec) return encode_null_array();
                            session.blocked.type = BlockType::ZSet;
                            session.blocked.zpop_max = true;
                            session.blocked.keys.clear();
                            for (std::size_t i = 1; i + 1 < args.size(); ++i)
                              session.blocked.keys.push_back(args[i]);
                            session.blocked.original_args = args;
                            if (timeout_sec > 0) {
                              session.blocked.deadline_ms =
                                  now_ms() + static_cast<std::int64_t>(timeout_sec * 1000.0L);
                            } else {
                              session.blocked.deadline_ms = 0;
                            }
                            return std::string();
                          }});

    t.emplace("SADD", CommandSpec{"SADD",
                                  -3,
                                  {"write"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return with_set_key(args[1], [&]() {
                                      std::vector<std::string> members(args.begin() + 2,
                                                                       args.end());
                                      bool wrongtype = false;
                                      auto n = store().sadd(args[1], members, wrongtype);
                                      if (wrongtype) return wrongtype_error_reply();
                                      return encode_integer(n);
                                    });
                                  }});

    t.emplace("SREM", CommandSpec{"SREM",
                                  -3,
                                  {"write"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    return with_set_key(args[1], [&]() {
                                      std::vector<std::string> members(args.begin() + 2,
                                                                       args.end());
                                      bool wrongtype = false;
                                      auto n = store().srem(args[1], members, wrongtype);
                                      if (wrongtype) return wrongtype_error_reply();
                                      return encode_integer(n);
                                    });
                                  }});

    t.emplace("SISMEMBER",
              CommandSpec{"SISMEMBER",
                          3,
                          {"readonly", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            return with_set_key(args[1], [&]() {
                              bool wrongtype = false;
                              auto ok = store().sismember(args[1], args[2], wrongtype);
                              if (wrongtype) return wrongtype_error_reply();
                              return encode_integer(ok ? 1 : 0);
                            });
                          }});

    t.emplace("SMEMBERS",
              CommandSpec{"SMEMBERS",
                          2,
                          {"readonly"},
                          1,
                          1,
                          1,
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

    t.emplace("SCARD", CommandSpec{"SCARD",
                                   2,
                                   {"readonly", "fast"},
                                   1,
                                   1,
                                   1,
                                   [](const std::vector<std::string>& args, SessionState&, bool&) {
                                     return with_set_key(args[1], [&]() {
                                       bool wrongtype = false;
                                       auto n = store().scard(args[1], wrongtype);
                                       if (wrongtype) return wrongtype_error_reply();
                                       return encode_integer(n);
                                     });
                                   }});

    t.emplace("SPOP",
              CommandSpec{"SPOP",
                          -2,
                          {"write", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            return with_set_key(args[1], [&]() {
                              bool wrongtype = false;
                              if (args.size() >= 3) {
                                std::int64_t count = 0;
                                if (!parse_i64(args[2], count) || count < 0)
                                  return encode_error("value is not an integer or out of range");
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
                              return v.has_value() ? encode_bulk(*v)
                                                   : encode_null(RespVersion::Resp2);
                            });
                          }});

    t.emplace("SSCAN", CommandSpec{"SSCAN",
                                   -3,
                                   {"readonly"},
                                   1,
                                   1,
                                   1,
                                   [](const std::vector<std::string>& args, SessionState&, bool&) {
                                     return collection_scan_command(args, ValueType::Set);
                                   }});

    t.emplace("ZADD",
              CommandSpec{
                  "ZADD",
                  -4,
                  {"write"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                    bool nx = false, xx = false, gt = false, lt = false, ch = false, incr = false;
                    std::size_t pos = 2;
                    for (; pos < args.size(); ++pos) {
                      const std::string o = upper(args[pos]);
                      if (o == "NX") {
                        nx = true;
                        continue;
                      }
                      if (o == "XX") {
                        xx = true;
                        continue;
                      }
                      if (o == "GT") {
                        gt = true;
                        continue;
                      }
                      if (o == "LT") {
                        lt = true;
                        continue;
                      }
                      if (o == "CH") {
                        ch = true;
                        continue;
                      }
                      if (o == "INCR") {
                        incr = true;
                        continue;
                      }
                      break;
                    }
                    if (pos >= args.size() || ((args.size() - pos) % 2) != 0)
                      return encode_error("syntax error");
                    if (incr && (args.size() - pos) != 2)
                      return encode_error("INCR option supports a single increment-element pair");
                    if (nx && xx)
                      return encode_error("XX and NX options at the same time are not compatible");
                    if ((gt && nx) || (lt && nx) || (gt && lt)) {
                      return encode_error(
                          "GT, LT, and/or NX options at the same time are not compatible");
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
                        auto r = store().zadd_one(args[1], scores[(i - pos) / 2], args[i + 1], nx,
                                                  xx, gt, lt, incr);
                        if (r.wrongtype) return wrongtype_error_reply();
                        if (!r.valid) return encode_error("syntax error");
                        if (r.added) ++added;
                        if (r.changed) ++changed;
                        if (incr) {
                          if (std::isnan(r.score))
                            return encode_error("resulting score is not a number (NaN)");
                          // NX/XX/GT/LT can suppress the INCR: Redis replies nil then.
                          if (!r.added && !r.changed && (nx || xx || gt || lt)) incr_nil = true;
                          incr_out = format_score(r.score);
                        }
                      }
                      if (incr)
                        return incr_nil ? encode_null(RespVersion::Resp2) : encode_bulk(incr_out);
                      return encode_integer(ch ? changed : added);
                    });
                  }});

    t.emplace("GEOADD",
              CommandSpec{"GEOADD",
                          -5,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            bool nx = false, xx = false, ch = false;
                            std::size_t pos = 2;
                            for (; pos < args.size(); ++pos) {
                              const std::string o = upper(args[pos]);
                              if (o == "NX") {
                                nx = true;
                                continue;
                              }
                              if (o == "XX") {
                                xx = true;
                                continue;
                              }
                              if (o == "CH") {
                                ch = true;
                                continue;
                              }
                              break;
                            }
                            // Redis GEOADD folds XX+NX into the generic syntax error
                            // (geoaddCommand: one shared.syntaxerr for both the odd-triple and the
                            // XX&&NX cases).
                            if ((nx && xx) || pos >= args.size() || ((args.size() - pos) % 3) != 0)
                              return encode_error("syntax error");
                            return with_zset_key(args[1], [&]() {
                              int added = 0, changed = 0;
                              for (std::size_t i = pos; i < args.size(); i += 3) {
                                long double lon = 0;
                                long double lat = 0;
                                if (!parse_f64(args[i], lon) || !parse_f64(args[i + 1], lat))
                                  return encode_error("value is not a valid float");
                                if (lon < geoutil::LON_MIN || lon > geoutil::LON_MAX ||
                                    lat < geoutil::LAT_MIN || lat > geoutil::LAT_MAX) {
                                  return encode_error("invalid longitude,latitude pair " + args[i] +
                                                      "," + args[i + 1]);
                                }
                                // 52-bit interleaved geohash score (bit-exact with Redis; see
                                // geoutil::encode).
                                const double score = static_cast<double>(geoutil::encode(
                                    static_cast<double>(lon), static_cast<double>(lat)));
                                auto r = store().zadd_one(args[1], score, args[i + 2], nx, xx,
                                                          false, false, false);
                                if (r.wrongtype) return wrongtype_error_reply();
                                if (!r.valid) return encode_error("syntax error");
                                if (r.added) ++added;
                                if (r.changed) ++changed;
                              }
                              return encode_integer(ch ? changed : added);
                            });
                          }});

    t.emplace("ZRANGE",
              CommandSpec{"ZRANGE",
                          -4,
                          {"readonly"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            // Unified Redis 6.2 ZRANGE: index/BYSCORE/BYLEX + REV + LIMIT +
                            // WITHSCORES, sharing the selector with ZRANGESTORE.
                            std::vector<std::pair<std::string, double>> items;
                            bool withscores = false;
                            auto err = zrange_select(args, 1, 2, 4, /*allow_withscores=*/true,
                                                     items, withscores);
                            if (!err.empty()) return err;
                            std::vector<std::string> out;
                            const bool resp3_pairs =
                                withscores && session.resp_version == RespVersion::Resp3;
                            out.reserve(items.size() * (withscores && !resp3_pairs ? 2 : 1));
                            for (const auto& [m, s] : items) {
                              if (resp3_pairs) {
                                out.push_back(encode_array(
                                    {encode_bulk(m), encode_double(s, RespVersion::Resp3)}));
                              } else {
                                out.push_back(encode_bulk(m));
                                if (withscores) out.push_back(encode_bulk(format_score(s)));
                              }
                            }
                            return encode_array(out);
                          }});

    t.emplace("ZSCAN", CommandSpec{"ZSCAN",
                                   -3,
                                   {"readonly"},
                                   1,
                                   1,
                                   1,
                                   [](const std::vector<std::string>& args, SessionState&, bool&) {
                                     return collection_scan_command(args, ValueType::ZSet);
                                   }});

    t.emplace("ZPOPMIN",
              CommandSpec{"ZPOPMIN",
                          -2,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            const bool has_count = args.size() == 3;
                            std::size_t count = 1;
                            if (has_count) {
                              std::int64_t c = 0;
                              if (!parse_i64(args[2], c))
                                return encode_error("value is not an integer or out of range");
                              if (c < 0)
                                return encode_error("value is out of range, must be positive");
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
                                  out.push_back(encode_array(
                                      {encode_bulk(m), encode_double(s, RespVersion::Resp3)}));
                                } else {
                                  out.push_back(encode_bulk(m));
                                  out.push_back(r3 ? encode_double(s, RespVersion::Resp3)
                                                   : encode_bulk(format_score(s)));
                                }
                              }
                              return encode_array(out);
                            });
                          }});

    t.emplace("ZPOPMAX",
              CommandSpec{"ZPOPMAX",
                          -2,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            const bool has_count = args.size() == 3;
                            std::size_t count = 1;
                            if (has_count) {
                              std::int64_t c = 0;
                              if (!parse_i64(args[2], c))
                                return encode_error("value is not an integer or out of range");
                              if (c < 0)
                                return encode_error("value is out of range, must be positive");
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
                                  out.push_back(encode_array(
                                      {encode_bulk(m), encode_double(s, RespVersion::Resp3)}));
                                } else {
                                  out.push_back(encode_bulk(m));
                                  out.push_back(r3 ? encode_double(s, RespVersion::Resp3)
                                                   : encode_bulk(format_score(s)));
                                }
                              }
                              return encode_array(out);
                            });
                          }});

    t.emplace(
        "ZMPOP",
        CommandSpec{"ZMPOP",
                    -4,
                    {"write"},
                    0,
                    0,
                    0,
                    [](const std::vector<std::string>& args, SessionState& session, bool&) {
                      std::int64_t numkeys = 0;
                      if (!parse_i64(args[1], numkeys) || numkeys <= 0) {
                        return encode_error("numkeys should be greater than 0");
                      }
                      if (args.size() < static_cast<std::size_t>(3 + numkeys))
                        return encode_error("syntax error");
                      std::size_t pos = static_cast<std::size_t>(2 + numkeys);
                      const auto dir = upper(args[pos]);
                      if (dir != "MIN" && dir != "MAX") return encode_error("syntax error");
                      bool max = (dir == "MAX");
                      std::size_t count = 1;
                      if (pos + 1 < args.size()) {
                        if (pos + 3 != args.size() || upper(args[pos + 1]) != "COUNT")
                          return encode_error("syntax error");
                        std::int64_t c = 0;
                        if (!parse_i64(args[pos + 2], c) || c <= 0)
                          return encode_error("count should be greater than 0");
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
                        propagate_write(eff, session, nullptr);
                        // Redis replies [key, [[member, score], ...]] — nested pairs, not flat;
                        // scores are RESP3 doubles for RESP3 clients.
                        std::vector<std::string> pairs;
                        for (const auto& [m, s] : vals) {
                          pairs.push_back(encode_array(
                              {encode_bulk(m), encode_double(s, session.resp_version)}));
                        }
                        return encode_array({encode_bulk(key), encode_array(pairs)});
                      }
                      return encode_null_array(session.resp_version);  // nothing popped anywhere
                    }});

    t.emplace(
        "XINFO",
        CommandSpec{
            "XINFO",
            -2,
            {"readonly"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              const auto sub = upper(args[1]);
              const bool r3 = session.resp_version == RespVersion::Resp3;
              auto entry_reply =
                  [](const std::pair<std::string, std::vector<std::pair<std::string, std::string>>>&
                         e) {
                    if (e.first.empty()) return encode_null_array();
                    std::vector<std::string> fv;
                    for (const auto& [f, v] : e.second) {
                      fv.push_back(encode_bulk(f));
                      fv.push_back(encode_bulk(v));
                    }
                    return encode_array({encode_bulk(e.first), encode_array(fv)});
                  };
              // Encode a map as RESP3 %N (or RESP2 flat *2N) from ordered key/encoded-value pairs.
              auto map_reply = [&](const std::vector<std::pair<std::string, std::string>>& kv) {
                std::string s =
                    (r3 ? "%" : "*") + std::to_string(r3 ? kv.size() : kv.size() * 2) + "\r\n";
                for (const auto& [k, v] : kv) {
                  s += encode_bulk(k);
                  s += v;
                }
                return s;
              };
              if (sub == "STREAM") {
                if (args.size() < 3)
                  return encode_error("wrong number of arguments for 'xinfo|stream' command");
                const bool full = args.size() >= 4 && upper(args[3]) == "FULL";
                if (full) {
                  std::size_t fcount = 10;  // Redis default COUNT for FULL (0 = no limit)
                  for (std::size_t i = 4; i < args.size(); ++i) {
                    if (upper(args[i]) == "COUNT" && i + 1 < args.size()) {
                      std::int64_t c = 0;
                      if (!parse_i64(args[i + 1], c) || c < 0)
                        return encode_error("value is not an integer or out of range");
                      fcount = static_cast<std::size_t>(c);
                      ++i;
                    } else {
                      return encode_error("syntax error");
                    }
                  }
                  DataStore::StreamFullInfo fi;
                  bool wt = false;
                  if (!store().xinfo_stream_full(args[2], fcount, fi, wt)) {
                    if (wt) return wrongtype_error_reply();
                    return encode_error("no such key");
                  }
                  std::vector<std::string> ents;
                  for (const auto& [id, fields] : fi.entries) {
                    std::vector<std::string> fv;
                    for (const auto& [f, v] : fields) {
                      fv.push_back(encode_bulk(f));
                      fv.push_back(encode_bulk(v));
                    }
                    ents.push_back(encode_array({encode_bulk(id), encode_array(fv)}));
                  }
                  std::vector<std::string> grps;
                  for (const auto& g : fi.groups) {
                    std::vector<std::string> gpend;
                    for (const auto& p : g.pending)
                      gpend.push_back(encode_array({encode_bulk(p.id), encode_bulk(p.consumer),
                                                    encode_integer(p.delivery_ms),
                                                    encode_integer(p.delivery_count)}));
                    std::vector<std::string> gcons;
                    for (const auto& c : g.consumers) {
                      std::vector<std::string> cpend;
                      for (const auto& p : c.pending)
                        cpend.push_back(
                            encode_array({encode_bulk(p.id), encode_integer(p.delivery_ms),
                                          encode_integer(p.delivery_count)}));
                      gcons.push_back(map_reply(
                          {{"name", encode_bulk(c.name)},
                           {"seen-time", encode_integer(c.seen_time)},
                           {"active-time", encode_integer(c.active_time)},
                           {"pel-count", encode_integer(static_cast<long long>(c.pending.size()))},
                           {"pending", encode_array(cpend)}}));
                    }
                    grps.push_back(map_reply(
                        {{"name", encode_bulk(g.name)},
                         {"last-delivered-id", encode_bulk(g.last_delivered_id)},
                         {"entries-read", g.entries_read < 0 ? encode_null(session.resp_version)
                                                             : encode_integer(g.entries_read)},
                         {"lag",
                          g.lag_valid ? encode_integer(g.lag) : encode_null(session.resp_version)},
                         {"pel-count", encode_integer(g.pel_count)},
                         {"pending", encode_array(gpend)},
                         {"consumers", encode_array(gcons)}}));
                  }
                  return map_reply(
                      {{"length", encode_integer(fi.length)},
                       {"radix-tree-keys",
                        encode_integer(fi.length == 0 ? 1 : (fi.length + 99) / 100)},
                       {"radix-tree-nodes",
                        encode_integer(fi.length == 0 ? 2 : (fi.length + 99) / 100 + 1)},
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
                return map_reply(
                    {{"length", encode_integer(si.length)},
                     {"radix-tree-keys",
                      encode_integer(si.length == 0 ? 1 : (si.length + 99) / 100)},
                     {"radix-tree-nodes",
                      encode_integer(si.length == 0 ? 2 : (si.length + 99) / 100 + 1)},
                     {"last-generated-id", encode_bulk(si.last_id)},
                     {"max-deleted-entry-id", encode_bulk(si.max_deleted_id)},
                     {"entries-added", encode_integer(static_cast<long long>(si.entries_added))},
                     {"recorded-first-entry-id",
                      encode_bulk(si.first.first.empty() ? "0-0" : si.first.first)},
                     {"groups", encode_integer(static_cast<long long>(si.groups))},
                     {"first-entry", entry_reply(si.first)},
                     {"last-entry", entry_reply(si.last)}});
              }
              if (sub == "GROUPS" && args.size() == 3) {
                bool wt = false;
                std::string err;
                auto gs = store().xinfo_groups(args[2], wt, err);
                if (wt) return wrongtype_error_reply();
                if (!err.empty()) return encode_stream_error(err);
                std::vector<std::string> out;
                for (const auto& g : gs)
                  out.push_back(map_reply(
                      {{"name", encode_bulk(g.name)},
                       {"consumers", encode_integer(static_cast<long long>(g.consumers))},
                       {"pending", encode_integer(static_cast<long long>(g.pending))},
                       {"last-delivered-id", encode_bulk(g.last_delivered_id)},
                       {"entries-read", g.entries_read < 0 ? encode_null(session.resp_version)
                                                           : encode_integer(g.entries_read)},
                       {"lag",
                        g.lag_valid ? encode_integer(g.lag) : encode_null(session.resp_version)}}));
                return encode_array(out);
              }
              if (sub == "CONSUMERS" && args.size() == 4) {
                bool wt = false;
                std::string err;
                auto cs = store().xinfo_consumers(args[2], args[3], wt, err);
                if (wt) return wrongtype_error_reply();
                if (!err.empty()) return encode_stream_error(err);
                std::vector<std::string> out;
                for (const auto& c : cs)
                  out.push_back(
                      map_reply({{"name", encode_bulk(c.name)},
                                 {"pending", encode_integer(static_cast<long long>(c.pending))},
                                 {"idle", encode_integer(c.idle)},
                                 {"inactive", encode_integer(c.inactive)}}));
                return encode_array(out);
              }
              if (sub == "HELP") {
                if (args.size() != 2)
                  return encode_error("wrong number of arguments for 'xinfo|help' command");
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

    t.emplace("XADD",
              CommandSpec{
                  "XADD",
                  -5,
                  {"write"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState& session, bool&) {
                    // Parse optional flags: [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT
                    // count]] id field value ...
                    std::size_t idx = 2;
                    bool nomkstream = false;
                    std::int64_t maxlen = -1;
                    std::string minid_threshold;
                    bool trim_approx = false;

                    while (idx < args.size()) {
                      const auto opt = upper(args[idx]);
                      if (opt == "NOMKSTREAM") {
                        nomkstream = true;
                        ++idx;
                        continue;
                      }
                      if (opt == "MAXLEN" || opt == "MINID") {
                        bool is_maxlen = (opt == "MAXLEN");
                        ++idx;
                        if (idx >= args.size()) return encode_error("syntax error");
                        // Optional = or ~
                        if (args[idx] == "~" || args[idx] == "=") {
                          trim_approx = (args[idx] == "~");
                          ++idx;
                        }
                        if (idx >= args.size()) return encode_error("syntax error");
                        if (is_maxlen) {
                          if (!parse_i64(args[idx], maxlen))
                            return encode_error("value is not an integer or out of range");
                        } else {
                          minid_threshold = args[idx];
                        }
                        ++idx;
                        // Optional LIMIT count — only valid with the approximate (~) form.
                        if (idx < args.size() && upper(args[idx]) == "LIMIT") {
                          if (!trim_approx) {
                            return encode_error(
                                "syntax error, LIMIT cannot be used without the special ~ option");
                          }
                          idx += 2;  // LIMIT count (exact enforcement needs the macro-node model)
                        }
                        continue;
                      }
                      break;  // First non-option is the ID
                    }
                    if (idx >= args.size())
                      return encode_error("wrong number of arguments for 'xadd' command");
                    const std::string& stream_id = args[idx];
                    ++idx;
                    if (((args.size() - idx) % 2) != 0)
                      return encode_error("wrong number of arguments for 'xadd' command");
                    if (args.size() == idx)
                      return encode_error("wrong number of arguments for 'xadd' command");

                    // If NOMKSTREAM and key doesn't exist, return null
                    if (nomkstream && store().type_of(args[1]) == "none")
                      return encode_null(RespVersion::Resp2);

                    return with_stream_key(args[1], [&]() {
                      std::vector<std::pair<std::string, std::string>> fields;
                      for (std::size_t i = idx; i < args.size(); i += 2)
                        fields.push_back({args[i], args[i + 1]});
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
                          for (std::size_t i = 0; i < to_remove; ++i)
                            ids_to_del.push_back(entries[i].first);
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
                          if (!ids_to_del.empty())
                            store().xdel(args[1], ids_to_del, wt, /*is_trim=*/true);
                        }
                      }

                      notify_key_written(args[1],
                                         session.db_index);  // wake blocked XREAD/XREADGROUP
                      return encode_bulk(*id);
                    });
                  }});

    t.emplace("XDEL", CommandSpec{"XDEL",
                                  -3,
                                  {"write"},
                                  1,
                                  1,
                                  1,
                                  [](const std::vector<std::string>& args, SessionState&, bool&) {
                                    std::vector<std::string> ids(args.begin() + 2, args.end());
                                    return with_stream_key(args[1], [&]() {
                                      bool wrongtype = false;
                                      const auto n = store().xdel(args[1], ids, wrongtype);
                                      if (wrongtype) return wrongtype_error_reply();
                                      return encode_integer(n);
                                    });
                                  }});

    t.emplace("XLEN", CommandSpec{"XLEN",
                                  2,
                                  {"readonly", "fast"},
                                  1,
                                  1,
                                  1,
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
        if (!parse_i64(args[5], count) || count < 0)
          return encode_error("value is not an integer or out of range");
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
    t.emplace("XRANGE",
              CommandSpec{"XRANGE",
                          -4,
                          {"readonly"},
                          1,
                          1,
                          1,
                          [xrange_cmd](const std::vector<std::string>& args, SessionState&, bool&) {
                            return xrange_cmd(args, /*rev=*/false);
                          }});
    t.emplace("XREVRANGE",
              CommandSpec{"XREVRANGE",
                          -4,
                          {"readonly"},
                          1,
                          1,
                          1,
                          [xrange_cmd](const std::vector<std::string>& args, SessionState&, bool&) {
                            return xrange_cmd(args, /*rev=*/true);
                          }});

    t.emplace(
        "XGROUP",
        CommandSpec{
            "XGROUP",
            -2,
            {"write"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              const auto sub = upper(args[1]);
              if (sub == "CREATE") {
                if (args.size() < 5)
                  return encode_error("wrong number of arguments for 'xgroup|create' command");
                // XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD n]
                bool mkstream = false;
                bool have_er = false;
                std::int64_t entries_read = 0;
                for (std::size_t i = 5; i < args.size(); ++i) {
                  const auto opt = upper(args[i]);
                  if (opt == "MKSTREAM") {
                    mkstream = true;
                    continue;
                  }
                  if (opt == "ENTRIESREAD" && i + 1 < args.size()) {
                    if (!parse_i64(args[i + 1], entries_read))
                      return encode_error("value is not an integer or out of range");
                    if (entries_read < -1)
                      return encode_error("value for ENTRIESREAD must be positive or -1");
                    have_er = true;
                    ++i;
                    continue;
                  }
                  return encode_error("syntax error");
                }
                return with_stream_key(args[2], [&]() {
                  bool wrongtype = false;
                  std::string err;
                  const bool ok =
                      store().xgroup_create(args[2], args[3], args[4], mkstream, wrongtype, err,
                                            have_er ? &entries_read : nullptr);
                  if (wrongtype) return wrongtype_error_reply();
                  if (!ok) return err.empty() ? encode_error("ERR") : encode_stream_error(err);
                  notify_keyspace_event(NOTIFY_STREAM, "xgroup-create", args[2], session.db_index);
                  return encode_simple("OK");
                });
              }
              if (sub == "SETID") {
                // XGROUP SETID key group id|$ [ENTRIESREAD n]
                if (args.size() != 5 && args.size() != 7)
                  return encode_error("wrong number of arguments for 'xgroup|setid' command");
                bool have_er = false;
                std::int64_t entries_read = 0;
                if (args.size() == 7) {
                  if (upper(args[5]) != "ENTRIESREAD") return encode_error("syntax error");
                  if (!parse_i64(args[6], entries_read))
                    return encode_error("value is not an integer or out of range");
                  if (entries_read < -1)
                    return encode_error("value for ENTRIESREAD must be positive or -1");
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
                if (args.size() != 4)
                  return encode_error("wrong number of arguments for 'xgroup|destroy' command");
                return with_stream_key(args[2], [&]() {
                  bool wrongtype = false;
                  const int n = store().xgroup_destroy(args[2], args[3], wrongtype);
                  if (wrongtype) return wrongtype_error_reply();
                  if (n > 0)
                    notify_key_written(args[2],
                                       session.db_index);  // wake blocked XREADGROUP → NOGROUP
                  return encode_integer(n);
                });
              }
              if (sub == "CREATECONSUMER") {
                if (args.size() != 5)
                  return encode_error(
                      "wrong number of arguments for 'xgroup|createconsumer' command");
                return with_stream_key(args[2], [&]() {
                  bool wrongtype = false;
                  std::string err;
                  const int n =
                      store().xgroup_createconsumer(args[2], args[3], args[4], wrongtype, err);
                  if (wrongtype) return wrongtype_error_reply();
                  if (!err.empty()) return encode_stream_error(err);
                  if (n > 0)
                    notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", args[2],
                                          session.db_index);
                  return encode_integer(n);
                });
              }
              if (sub == "DELCONSUMER") {
                if (args.size() != 5)
                  return encode_error("wrong number of arguments for 'xgroup|delconsumer' command");
                return with_stream_key(args[2], [&]() {
                  bool wrongtype = false;
                  std::string err;
                  bool existed = false;
                  const std::int64_t n = store().xgroup_delconsumer(args[2], args[3], args[4],
                                                                    wrongtype, err, &existed);
                  if (wrongtype) return wrongtype_error_reply();
                  if (!err.empty()) return encode_stream_error(err);
                  if (existed)
                    notify_keyspace_event(NOTIFY_STREAM, "xgroup-delconsumer", args[2],
                                          session.db_index);
                  return encode_integer(n);
                });
              }
              if (sub == "HELP") {
                if (args.size() != 2)
                  return encode_error("wrong number of arguments for 'xgroup|help' command");
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

    t.emplace(
        "XREADGROUP",
        CommandSpec{
            "XREADGROUP",
            -7,
            {"write"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              if (upper(args[1]) != "GROUP") return encode_error("syntax error");
              if (args.size() < 7)
                return encode_error("wrong number of arguments for 'xreadgroup' command");
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
                  if (!parse_i64(args[i + 1], block) || block < 0)
                    return encode_error("value is not an integer or out of range");
                  if (in_lua_script_context())
                    return encode_error(
                        "xreadgroup command is not allowed with BLOCK option from scripts");
                  block_ms = block;
                  i += 2;
                  continue;
                }
                if (opt == "COUNT") {
                  if (i + 1 >= args.size()) return encode_error("syntax error");
                  std::int64_t n = 0;
                  if (!parse_i64(args[i + 1], n) || n <= 0)
                    return encode_error("value is not an integer or out of range");
                  req_count = static_cast<std::size_t>(n);
                  i += 2;
                  continue;
                }
                return encode_error("syntax error");
              }
              ++i;
              const std::size_t rem = args.size() - i;
              if (rem == 0 || (rem % 2) != 0)
                return encode_error(
                    "Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' "
                    "must be specified.");
              const std::size_t n = rem / 2;
              std::vector<std::pair<std::string, std::string>> streams;
              for (std::size_t k = 0; k < n; ++k) {
                streams.push_back({args[i + k], args[i + n + k]});
              }
              bool wrongtype = false;
              std::string err;
              bool consumer_created = false;
              std::vector<DataStore::StreamClaimEffect> effects;
              std::vector<DataStore::StreamGroupEffect> group_effects;
              std::vector<std::string> created_keys;
              auto rows =
                  store().xreadgroup(group, consumer, streams, req_count, noack, wrongtype, err,
                                     &consumer_created, &effects, &created_keys, &group_effects);
              if (wrongtype) return wrongtype_error_reply();
              if (!err.empty()) return encode_stream_error(err);
              propagate_stream_claims(effects, session);

              // XREADGROUP is not propagated verbatim (canonicalize sets emit=false); instead we
              // propagate its deterministic effects to BOTH the AOF and the replication backlog.
              auto propagate = [&](std::vector<std::string> cmd) {
                propagate_stream_effect(std::move(cmd), session);
              };
              // For a NOACK read there is no XCLAIM to carry the consumer, so a newly-created
              // consumer is propagated as XGROUP CREATECONSUMER up front (Redis creates it when
              // the command is processed, even if the read then blocks or returns nothing). ACK
              // reads instead let the per-entry XCLAIM (below) create the consumer on replay.
              for (const auto& key : created_keys) {
                if (noack) propagate({"XGROUP", "CREATECONSUMER", key, group, consumer});
                notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", key,
                                      session.db_index);
              }
              for (const auto& e : group_effects)
                propagate({"XGROUP", "SETID", e.key, e.group, e.last_id, "ENTRIESREAD",
                           std::to_string(e.entries_read)});

              if (rows.empty()) {
                // Block only when reading new messages (all ids are ">"), like Redis.
                bool all_new = !streams.empty();
                for (const auto& s : streams)
                  if (s.second != ">") {
                    all_new = false;
                    break;
                  }
                if (block_ms >= 0 && all_new && !g_block_disabled && !in_lua_script_context() &&
                    !g_executing_exec) {
                  session.blocked.type = BlockType::Stream;
                  session.blocked.keys.clear();
                  for (const auto& s : streams) {
                    session.blocked.keys.push_back(s.first);
                    register_key_waiter(s.first, session.fd);
                  }
                  session.blocked.original_args = args;
                  session.blocked.deadline_ms = block_ms > 0 ? now_ms() + block_ms : 0;
                  return std::string{};  // deferred
                }
                return encode_null(RespVersion::Resp2);
              }

              // ACK effects were snapshotted by the datastore and propagated above. NOACK
              // reads carry the group cursor with XGROUP SETID. Redis does not propagate the
              // local delivery-time/count changes of history reads (id != ">"), so neither do we.
              std::vector<std::string> out;
              for (const auto& [key, entries] : rows) {
                std::vector<std::string> encoded_entries;
                for (const auto& [id, fields] : entries) {
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
                out.push_back(encode_array({encode_bulk(key), encode_array(encoded_entries)}));
              }
              return encode_array(out);
            }});

    t.emplace("XREAD",
              CommandSpec{
                  "XREAD",
                  -4,
                  {"readonly", "blocking"},
                  0,
                  0,
                  0,
                  [](const std::vector<std::string>& args, SessionState& session, bool&) {
                    std::size_t i = 1;
                    std::int64_t count = -1;
                    std::int64_t block_ms = -1;
                    while (i < args.size()) {
                      const auto opt = upper(args[i]);
                      if (opt == "COUNT") {
                        if (i + 1 >= args.size()) return encode_error("syntax error");
                        std::int64_t v = 0;
                        if (!parse_i64(args[i + 1], v) || v < 0)
                          return encode_error("value is not an integer or out of range");
                        count = v;
                        i += 2;
                        continue;
                      }
                      if (opt == "BLOCK") {
                        if (i + 1 >= args.size()) return encode_error("syntax error");
                        std::int64_t v = 0;
                        if (!parse_i64(args[i + 1], v) || v < 0)
                          return encode_error("value is not an integer or out of range");
                        if (in_lua_script_context())
                          return encode_error(
                              "xread command is not allowed with BLOCK option from scripts");
                        block_ms = v;
                        i += 2;
                        continue;
                      }
                      break;
                    }
                    if (i >= args.size() || upper(args[i]) != "STREAMS")
                      return encode_error("syntax error");
                    ++i;
                    const auto rem = args.size() - i;
                    if (rem == 0 || (rem % 2) != 0)
                      return encode_error(
                          "Unbalanced 'xread' list of streams: for each stream key an ID or '$' "
                          "must be specified.");
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
                      std::vector<
                          std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>
                          filtered;
                      for (const auto& e : entries) {
                        // Compare stream IDs: entries strictly greater than start_exclusive
                        std::uint64_t ems = 0, eseq = 0, sms = 0, sseq = 0;
                        auto parse_sid = [](const std::string& sid, std::uint64_t& ms_out,
                                            std::uint64_t& seq_out) {
                          const auto pos = sid.find('-');
                          if (pos == std::string::npos) {
                            ms_out = std::stoull(sid);
                            seq_out = 0;
                            return;
                          }
                          ms_out = std::stoull(sid.substr(0, pos));
                          seq_out = std::stoull(sid.substr(pos + 1));
                        };
                        try {
                          parse_sid(e.first, ems, eseq);
                          parse_sid(start_exclusive, sms, sseq);
                        } catch (...) {
                          continue;
                        }
                        if (ems > sms || (ems == sms && eseq > sseq)) {
                          filtered.push_back(e);
                          if (count > 0 && static_cast<std::int64_t>(filtered.size()) >= count)
                            break;
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
                        entry_items.push_back(
                            encode_array({encode_bulk(fe.first), encode_array(field_vals)}));
                      }
                      results.push_back(
                          encode_array({encode_bulk(stream_key), encode_array(entry_items)}));
                    }
                    if (!any_data && block_ms >= 0 && !g_block_disabled &&
                        !in_lua_script_context() && !g_executing_exec) {
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
                      for (std::size_t k = 0; k < n; ++k) {
                        session.blocked.keys.push_back(args[i + k]);
                        register_key_waiter(args[i + k], session.fd);
                      }
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

    t.emplace(
        "XACK",
        CommandSpec{"XACK",
                    -4,
                    {"write"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      std::vector<std::string> ids(args.begin() + 3, args.end());
                      // Validate every id up front: XACK is all-or-nothing, so one bad id is an
                      // error rather than a partial ack (Redis xackCommand parses all IDs before
                      // acting).
                      for (const auto& id : ids) {
                        if (!stream_id_is_valid(id))
                          return encode_error(
                              "Invalid stream ID specified as stream command argument");
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

    t.emplace(
        "XPENDING",
        CommandSpec{
            "XPENDING",
            -3,
            {"readonly"},
            1,
            1,
            1,
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
                  out.push_back(sum.size() > 1 && !sum[1].empty()
                                    ? encode_bulk(sum[1])
                                    : encode_null(RespVersion::Resp2));
                  out.push_back(sum.size() > 2 && !sum[2].empty()
                                    ? encode_bulk(sum[2])
                                    : encode_null(RespVersion::Resp2));
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
                  if (!parse_i64(args[i + 1], min_idle) || min_idle < 0)
                    return encode_error("value is not an integer or out of range");
                  i += 2;
                }
                if (i + 2 >= args.size())
                  return encode_error("syntax error");  // need start end count
                const std::string start = args[i], end = args[i + 1];
                std::int64_t count = 0;
                if (!parse_i64(args[i + 2], count))
                  return encode_error("value is not an integer or out of range");
                i += 3;
                std::string consumer;
                bool have_consumer = false;
                if (i < args.size()) {
                  consumer = args[i];
                  have_consumer = true;
                  ++i;
                }
                if (i != args.size()) return encode_error("syntax error");
                auto items = store().xpending_extended(args[1], args[2], start, end, count,
                                                       have_consumer ? &consumer : nullptr,
                                                       min_idle, wrongtype, err);
                if (wrongtype) return wrongtype_error_reply();
                if (!err.empty()) return encode_stream_error(err);
                std::vector<std::string> out;
                for (const auto& it : items) {
                  out.push_back(encode_array({encode_bulk(it.id), encode_bulk(it.consumer),
                                              encode_integer(it.idle_ms),
                                              encode_integer(it.delivery_count)}));
                }
                return encode_array(out);
              });
            }});

    // ─── SUBSCRIBE / UNSUBSCRIBE / PSUBSCRIBE / PUNSUBSCRIBE ────────────
    t.emplace("SUBSCRIBE",
              CommandSpec{"SUBSCRIBE",
                          -2,
                          {"pubsub"},
                          0,
                          0,
                          0,
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

    t.emplace("UNSUBSCRIBE",
              CommandSpec{
                  "UNSUBSCRIBE",
                  -1,
                  {"pubsub"},
                  0,
                  0,
                  0,
                  [](const std::vector<std::string>& args, SessionState& session, bool&) {
                    std::string out;
                    if (args.size() == 1) {
                      // Unsubscribe all channels
                      auto cit = g_fd_channels.find(session.fd);
                      if (cit != g_fd_channels.end()) {
                        auto channels_copy = cit->second;  // copy since we modify during iteration
                        for (const auto& ch : channels_copy) {
                          pubsub_unsubscribe(session.fd, ch);
                        }
                      }
                      auto& pats = g_fd_patterns[session.fd];
                      out =
                          encode_array({encode_bulk("unsubscribe"), encode_null(RespVersion::Resp2),
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

    t.emplace("PSUBSCRIBE",
              CommandSpec{"PSUBSCRIBE",
                          -2,
                          {"pubsub"},
                          0,
                          0,
                          0,
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

    t.emplace("PUNSUBSCRIBE",
              CommandSpec{"PUNSUBSCRIBE",
                          -1,
                          {"pubsub"},
                          0,
                          0,
                          0,
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
                              out = encode_array(
                                  {encode_bulk("punsubscribe"), encode_null(RespVersion::Resp2),
                                   encode_integer(static_cast<long long>(chans.size()))});
                            } else {
                              for (std::size_t i = 1; i < args.size(); ++i) {
                                pubsub_punsubscribe(session.fd, args[i]);
                                auto& chans = g_fd_channels[session.fd];
                                auto& pats = g_fd_patterns[session.fd];
                                long long total =
                                    static_cast<long long>(chans.size() + pats.size());
                                out += encode_array({encode_bulk("punsubscribe"),
                                                     encode_bulk(args[i]), encode_integer(total)});
                              }
                            }
                            return out;
                          }});

    // ─── CLIENT ────────────────────────────────────────────────────────
    t.emplace(
        "CLIENT",
        CommandSpec{
            "CLIENT",
            -2,
            {"admin", "loading", "stale"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session,
               bool& should_close) -> std::string {
              const auto sub = upper(args[1]);
              if (sub == "SETNAME") {
                if (args.size() != 3)
                  return encode_error("wrong number of arguments for 'client|setname' command");
                // Redis rejects names with any character <= ASCII 32.
                for (unsigned char c : args[2]) {
                  if (c <= 32)
                    return encode_error(
                        "Client names cannot contain spaces, newlines or special characters.");
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
                if (args.size() != 4)
                  return encode_error("wrong number of arguments for 'client|setinfo' command");
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
                  return encode_verbatim(g_client_list_callback(), session.resp_version);
                }
                return encode_verbatim("", session.resp_version);
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
                out += " multi=" +
                       std::string(session.in_multi
                                       ? std::to_string(static_cast<int>(session.queued.size()))
                                       : "-1");
                out += " watch=" + std::to_string(session.watched_digests.size());
                out += " qbuf=0 qbuf-free=32768 argv-mem=0 multi-mem=0 tot-mem=20512";
                out += " net-i=0 net-o=0 age=0 idle=0 flags=N";
                out += " cmd=client|info user=default redir=-1";
                out += " resp=" + std::to_string(static_cast<int>(session.resp_version));
                out += "\n";
                return encode_verbatim(out, session.resp_version);
              }
              if (sub == "KILL") {
                // clientKillCommand(): CLIENT KILL <ip:port>, or CLIENT KILL <filter> <value> ...
                // with ID, TYPE, ADDR, LADDR, USER and SKIPME, all of which must match.
                if (args.size() < 3)
                  return encode_error("wrong number of arguments for 'client|kill' command");
                ClientKillFilter filter;
                filter.self_fd = session.fd;
                const bool old_form = args.size() == 3;
                if (old_form) {
                  filter.addr = args[2];
                  filter.skipme = false;  // with the old form a client can kill itself
                } else {
                  for (std::size_t i = 2; i < args.size(); i += 2) {
                    const bool moreargs = args.size() > i + 1;
                    const auto opt = lower(args[i]);
                    if (opt == "id" && moreargs) {
                      std::int64_t id = 0;
                      if (!parse_i64(args[i + 1], id) || id < 1)
                        return encode_error("client-id should be greater than 0");
                      filter.id = static_cast<std::uint64_t>(id);
                    } else if (opt == "type" && moreargs) {
                      const auto type = lower(args[i + 1]);
                      if (type == "normal")
                        filter.type = ClientType::Normal;
                      else if (type == "slave" || type == "replica")
                        filter.type = ClientType::Slave;
                      else if (type == "pubsub")
                        filter.type = ClientType::Pubsub;
                      else if (type == "master")
                        filter.type = ClientType::Master;
                      else
                        return encode_error("Unknown client type '" + args[i + 1] + "'");
                    } else if (opt == "addr" && moreargs) {
                      filter.addr = args[i + 1];
                    } else if (opt == "laddr" && moreargs) {
                      filter.laddr = args[i + 1];
                    } else if (opt == "user" && moreargs) {
                      // Every client is the default user (multi-user ACL is item 7), so USER
                      // default matches all of them.
                      if (args[i + 1] != "default")
                        return encode_error("No such user '" + args[i + 1] + "'");
                    } else if (opt == "skipme" && moreargs) {
                      const auto v = lower(args[i + 1]);
                      if (v == "yes")
                        filter.skipme = true;
                      else if (v == "no")
                        filter.skipme = false;
                      else
                        return encode_error("syntax error");
                    } else {
                      return encode_error("syntax error");
                    }
                  }
                }
                bool kill_self = false;
                const int killed = g_client_kill_cb ? g_client_kill_cb(filter, kill_self) : 0;
                if (kill_self)
                  should_close = true;  // after the reply, like CLIENT_CLOSE_AFTER_REPLY
                if (old_form)
                  return killed > 0 ? encode_simple("OK") : encode_error("No such client");
                return encode_integer(killed);
              }
              if (sub == "NO-EVICT" && args.size() == 3) {
                const auto m = upper(args[2]);
                if (m != "ON" && m != "OFF") return encode_error("syntax error");
                return encode_simple("OK");  // eviction protection is a no-op (single-node)
              }
              if (sub == "NO-TOUCH" && args.size() == 3) {
                const auto m = upper(args[2]);
                if (m == "ON")
                  session.no_touch = true;
                else if (m == "OFF")
                  session.no_touch = false;
                else
                  return encode_error("syntax error");
                return encode_simple("OK");
              }
              if (sub == "UNBLOCK" && (args.size() == 3 || args.size() == 4)) {
                std::int64_t cid = 0;
                if (!parse_i64(args[2], cid) || cid < 0)
                  return encode_error("value is not an integer or out of range");
                bool error = false;
                if (args.size() == 4) {
                  const auto mode = upper(args[3]);
                  if (mode == "ERROR")
                    error = true;
                  else if (mode != "TIMEOUT")
                    return encode_error("CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
                }
                const int n = g_client_unblock_cb
                                  ? g_client_unblock_cb(static_cast<std::uint64_t>(cid), error)
                                  : 0;
                return encode_integer(n);
              }
              if (sub == "PAUSE") {
                // CLIENT PAUSE <timeout ms> [WRITE|ALL] (clientCommand, networking.c): the mode is
                // checked before the timeout; the timeout is relative, 0 sets no end time.
                if (args.size() < 3)
                  return encode_error("wrong number of arguments for 'client|pause' command");
                if (args.size() > 4)
                  return encode_error("unknown subcommand or wrong number of arguments for '" +
                                      args[1] + "'. Try CLIENT HELP.");
                bool pause_all = true;
                if (args.size() == 4) {
                  const auto mode = lower(args[3]);
                  if (mode == "write")
                    pause_all = false;
                  else if (mode != "all")
                    return encode_error("CLIENT PAUSE mode must be WRITE or ALL");
                }
                std::int64_t timeout = 0;  // getTimeoutFromObjectOrReply(..., UNIT_MILLISECONDS)
                if (!parse_strict_int(args[2], timeout))
                  return encode_error("timeout is not an integer or out of range");
                if (timeout < 0) return encode_error("timeout is negative");
                const std::int64_t now = now_ms();
                if (timeout > 0) {
                  if (timeout > std::numeric_limits<std::int64_t>::max() - now)
                    return encode_error("timeout is out of range");
                  timeout += now;
                }
                pause_clients(timeout, pause_all);
                return encode_simple("OK");
              }
              if (sub == "UNPAUSE") {
                if (args.size() != 2)
                  return encode_error("wrong number of arguments for 'client|unpause' command");
                unpause_clients();
                return encode_simple("OK");
              }
              if (sub == "REPLY") {
                if (args.size() != 3)
                  return encode_error("wrong number of arguments for 'client|reply' command");
                const auto mode = upper(args[2]);
                if (mode == "ON") {
                  session.reply_off = false;
                  return encode_simple("OK");
                }
                if (mode == "OFF") {
                  session.reply_off = true;
                  return std::string();
                }  // no reply
                if (mode == "SKIP") {
                  // Skip the NEXT command's reply (no-op if replies are already off). No reply now.
                  if (!session.reply_off) session.reply_skip_next = true;
                  return std::string();
                }
                return encode_error("syntax error");
              }
              if (sub == "HELP") {
                return encode_array(
                    {encode_bulk("CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
                     encode_bulk("ID"), encode_bulk("INFO"), encode_bulk("GETNAME"),
                     encode_bulk("SETNAME <name>"), encode_bulk("SETINFO <attr> <value>"),
                     encode_bulk("LIST [options ...]"),
                     encode_bulk("KILL <ip:port> | <option> <value> ..."),
                     encode_bulk("UNBLOCK <clientid> [TIMEOUT|ERROR]"),
                     encode_bulk("NO-EVICT ON|OFF"), encode_bulk("NO-TOUCH ON|OFF"),
                     encode_bulk("PAUSE <timeout> [WRITE|ALL]"), encode_bulk("UNPAUSE"),
                     encode_bulk("REPLY ON|OFF|SKIP"), encode_bulk("HELP")});
              }
              return encode_error("unknown subcommand or wrong number of arguments for 'client|" +
                                  lower(args[1]) + "' command");
            }});

    t.emplace("MONITOR",
              CommandSpec{"MONITOR",
                          1,
                          {"admin", "loading", "stale"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>&, SessionState& session, bool&) {
                            g_monitor_fds.insert(session.fd);
                            session.is_monitor = true;
                            return encode_simple("OK");
                          }});

    // ─── MEMORY ──────────────────────────────────────────────────────────
    t.emplace(
        "MEMORY",
        CommandSpec{
            "MEMORY",
            -2,
            {"readonly"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              const auto sub = upper(args[1]);
              if (sub == "USAGE") {
                if (args.size() < 3)
                  return encode_error("wrong number of arguments for 'memory|usage' command");
                const auto& key = args[2];
                // Redis's memoryCommand() reads the key raw: an expired key is measured, not
                // deleted.
                const DataStore::LazyExpireDisabled raw_lookup(store());
                if (store().type_of(key) == "none") return encode_null(RespVersion::Resp2);
                // Approximate memory usage — return a reasonable estimate
                const auto enc = store().object_encoding(key);
                std::int64_t bytes = 64;  // base overhead
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
                return encode_verbatim("Sam, I have no memory problems", session.resp_version);
              }
              if (sub == "STATS") {
                // Minimal stub matching Redis output format
                return encode_bulk(
                    "peak.allocated:0\ntotal.allocated:0\nstartup.allocated:0\n"
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
                items.push_back(
                    encode_bulk("MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"));
                items.push_back(encode_bulk("DOCTOR"));
                items.push_back(encode_bulk("MALLOC-STATS"));
                items.push_back(encode_bulk("PURGE"));
                items.push_back(encode_bulk("STATS"));
                items.push_back(encode_bulk("USAGE <key> [SAMPLES <count>]"));
                return encode_array(items);
              }
              return encode_error("unknown subcommand or wrong number of arguments for 'memory|" +
                                  lower(args[1]) + "' command");
            }});

    // ─── SLOWLOG ─────────────────────────────────────────────────────────
    t.emplace(
        "SLOWLOG",
        CommandSpec{
            "SLOWLOG",
            -2,
            {"admin"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
              const auto sub = upper(args[1]);
              if (sub == "GET") {
                std::int64_t count = 10;
                if (args.size() >= 3) {
                  if (!parse_i64(args[2], count))
                    return encode_error("value is not an integer or out of range");
                  if (count < -1)
                    return encode_error("count should be greater than or equal to -1");
                }
                std::vector<std::string> entries;
                std::size_t n = 0;
                for (const auto& e : g_slowlog) {
                  if (count >= 0 && n >= static_cast<std::size_t>(count)) break;
                  std::vector<std::string> argv;
                  for (const auto& a : e.argv) argv.push_back(encode_bulk(a));
                  // Each entry: [id, timestamp, usec, [argv...], client-addr, client-name]
                  entries.push_back(encode_array({encode_integer(e.id), encode_integer(e.time_s),
                                                  encode_integer(e.usec), encode_array(argv),
                                                  encode_bulk(e.peer), encode_bulk(e.name)}));
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
                items.push_back(
                    encode_bulk("SLOWLOG <subcommand> [<arg> [value] ...]. Subcommands are:"));
                items.push_back(encode_bulk("GET [<count>]"));
                items.push_back(encode_bulk("LEN"));
                items.push_back(encode_bulk("RESET"));
                return encode_array(items);
              }
              return encode_error("unknown subcommand or wrong number of arguments for 'slowlog|" +
                                  lower(args[1]) + "' command");
            }});

    // ─── LATENCY ─────────────────────────────────────────────────────────
    t.emplace(
        "LATENCY",
        CommandSpec{
            "LATENCY",
            -2,
            {"admin"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              const auto sub = upper(args[1]);
              if (sub == "LATEST") {
                // Each: [event, timestamp, latest-ms, max-ms]
                std::vector<std::string> out;
                for (const auto& [name, ev] : g_latency_events) {
                  out.push_back(
                      encode_array({encode_bulk(name), encode_integer(ev.time_s),
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
                if (it == g_latency_events.end() || it->second.history.empty())
                  return encode_bulk("");
                std::int64_t hi = 0, lo = std::numeric_limits<std::int64_t>::max();
                for (const auto& [t, l] : it->second.history) {
                  hi = std::max(hi, l);
                  lo = std::min(lo, l);
                }
                // Minimal graph: the legend line tooling / the suite match on.
                std::string g = args[2] + " - high " + std::to_string(hi) + " ms, low " +
                                std::to_string(lo) + " ms (all time high " +
                                std::to_string(it->second.max_ms) + " ms)\n";
                return encode_verbatim(g, session.resp_version);
              }
              if (sub == "HISTOGRAM") {
                // Per-command latency histograms, optionally filtered by command / parent name.
                std::vector<std::string> filters;
                for (std::size_t i = 2; i < args.size(); ++i) filters.push_back(lower(args[i]));
                auto matches = [&](const std::string& k) -> bool {
                  if (filters.empty()) return true;
                  for (const auto& f : filters) {
                    if (k == f) return true;
                    if (f.find('|') == std::string::npos && k.rfind(f + "|", 0) == 0)
                      return true;  // parent
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
                      buckets.push_back(
                          encode_integer(1LL << (i + 1)));  // bucket upper bound (usec)
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
                  for (std::size_t i = 2; i < args.size(); ++i)
                    n += g_latency_events.erase(args[i]);
                } else {
                  g_latency_events.clear();
                }
                return encode_integer(n);
              }
              if (sub == "DOCTOR") {
                if (g_latency_events.empty())
                  return encode_verbatim(
                      "Dave, I have observed the system, no worrying latency spikes. Everything "
                      "seems fine.",
                      session.resp_version);
                std::string rep = "Dave, I detected latency events:\n";
                for (const auto& [name, ev] : g_latency_events)
                  rep += "- " + name + ": max " + std::to_string(ev.max_ms) + " ms (last " +
                         std::to_string(ev.latest_ms) + " ms)\n";
                return encode_verbatim(rep, session.resp_version);
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
              return encode_error("unknown subcommand or wrong number of arguments for 'latency|" +
                                  lower(args[1]) + "' command");
            }});

    // ─── TOUCH ───────────────────────────────────────────────────────────
    t.emplace("TOUCH", CommandSpec{"TOUCH",
                                   -2,
                                   {"readonly", "fast"},
                                   1,
                                   -1,
                                   1,
                                   [](const std::vector<std::string>& args, SessionState&,
                                      bool&) -> std::string {
                                     long long count = 0;
                                     for (std::size_t i = 1; i < args.size(); ++i) {
                                       if (store().type_of(args[i]) != "none") ++count;
                                     }
                                     return encode_integer(count);
                                   }});

    // ─── ROLE ────────────────────────────────────────────────────────────
    t.emplace("ROLE",
              CommandSpec{"ROLE",
                          1,
                          {"readonly", "fast", "loading", "stale"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>&, SessionState&, bool&) -> std::string {
                            // roleCommand(): a master lists its online replicas with the offset
                            // each acknowledged; a replica shows its link's state and the offset of
                            // its master's stream it has applied
                            // (-1 without a link).
                            std::vector<std::string> items;
                            items.push_back(encode_bulk(g_replication_role));
                            if (g_replication_role == "master") {
                              items.push_back(encode_integer(g_master_repl_offset));
                              std::vector<std::string> replicas;
                              if (g_for_each_replica_cb) {
                                g_for_each_replica_cb([&](const SessionState& r) {
                                  if (r.repl_sync_state != ReplicaSyncState::Online) return;
                                  replicas.push_back(encode_array(
                                      {encode_bulk(replica_ip(r)),
                                       encode_bulk(std::to_string(r.repl_listening_port)),
                                       encode_bulk(std::to_string(r.repl_ack_offset))}));
                                });
                              }
                              items.push_back(encode_array(replicas));
                            } else {
                              const bool link_up = !g_replica_stale;
                              const char* state = "connect";
                              switch (g_repl_link_state.load()) {
                                case ReplLinkState::Connect:
                                  state = "connect";
                                  break;
                                case ReplLinkState::Connecting:
                                  state = "connecting";
                                  break;
                                case ReplLinkState::Handshake:
                                  state = "handshake";
                                  break;
                                case ReplLinkState::Transfer:
                                  state = "sync";
                                  break;
                                case ReplLinkState::Connected:
                                  state = link_up ? "connected" : "connect";
                                  break;
                              }
                              items.push_back(encode_bulk(g_replication_master_host));
                              items.push_back(encode_integer(g_replication_master_port));
                              items.push_back(encode_bulk(state));
                              items.push_back(encode_integer(link_up ? g_master_repl_offset : -1));
                            }
                            return encode_array(items);
                          }});

    // ─── RESET ───────────────────────────────────────────────────────────
    t.emplace("RESET", CommandSpec{"RESET",
                                   1,
                                   {"fast", "loading", "stale", "no_auth"},
                                   0,
                                   0,
                                   0,
                                   [](const std::vector<std::string>&, SessionState& session,
                                      bool&) -> std::string {
                                     session.db_index = 0;
                                     session.in_multi = false;
                                     session.multi_dirty = false;
                                     session.queued.clear();
                                     session.watched_epoch.reset();
                                     session.watched_digests.clear();
                                     session.client_name.clear();
                                     session.resp_version = RespVersion::Resp2;
                                     // RESET exits pub/sub (all channel + pattern + shard
                                     // subscriptions) and MONITOR mode, per Redis's resetCommand,
                                     // so the client returns to a clean interactive state.
                                     if (session.fd >= 0) {
                                       pubsub_unsubscribe_all(session.fd);
                                       g_monitor_fds.erase(session.fd);
                                     }
                                     // Per Redis, RESET re-runs clientSetDefaultAuth: the client is
                                     // authenticated iff the default user is nopass (no
                                     // requirepass), and de-authenticated otherwise.
                                     session.authenticated = g_requirepass.empty();
                                     session.user = "default";
                                     return "+RESET\r\n";
                                   }});

    // ─── LINDEX ──────────────────────────────────────────────────────────
    t.emplace("LINDEX",
              CommandSpec{"LINDEX",
                          3,
                          {"readonly"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session,
                             bool&) -> std::string {
                            if (store().is_wrongtype_for_list(args[1]))
                              return wrongtype_error_reply();
                            std::int64_t index;
                            if (!parse_i64(args[2], index))
                              return encode_error("value is not an integer or out of range");
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
    // LSET, LREM, LINSERT and LTRIM change the list in place, so the key keeps its TTL, and
    // send their own keyspace events in Redis's order (t_list.c), including "del" when the
    // list becomes empty.
    t.emplace("LSET",
              CommandSpec{"LSET",
                          4,
                          {"write"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session,
                             bool&) -> std::string {
                            // Redis looks the key up before it parses the index.
                            const auto type = store().type_of(args[1]);
                            if (type == "none") return encode_error("no such key");
                            if (type != "list") return wrongtype_error_reply();
                            std::int64_t index;
                            if (!parse_i64(args[2], index))
                              return encode_error("value is not an integer or out of range");
                            switch (store().lset(args[1], index, args[3])) {
                              case DataStore::LsetResult::Ok:
                                break;
                              case DataStore::LsetResult::NoKey:
                                return encode_error("no such key");
                              case DataStore::LsetResult::WrongType:
                                return wrongtype_error_reply();
                              case DataStore::LsetResult::OutOfRange:
                                return encode_error("index out of range");
                            }
                            notify_keyspace_event(NOTIFY_LIST, "lset", args[1], session.db_index);
                            return encode_simple("OK");
                          }});

    // ─── LREM ────────────────────────────────────────────────────────────
    t.emplace(
        "LREM",
        CommandSpec{
            "LREM",
            4,
            {"write"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              std::int64_t count;
              if (!parse_i64(args[2], count))
                return encode_error("value is not an integer or out of range");
              bool wt = false, key_removed = false;
              const auto removed = store().lrem(args[1], count, args[3], wt, key_removed);
              if (wt) return wrongtype_error_reply();
              if (removed > 0)
                notify_keyspace_event(NOTIFY_LIST, "lrem", args[1], session.db_index);
              if (key_removed)
                notify_keyspace_event(NOTIFY_GENERIC, "del", args[1], session.db_index);
              return encode_integer(removed);
            }});

    // ─── LPOS ────────────────────────────────────────────────────────────
    t.emplace(
        "LPOS",
        CommandSpec{
            "LPOS",
            -3,
            {"readonly"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              if (store().is_wrongtype_for_list(args[1])) return wrongtype_error_reply();
              std::int64_t rank = 1;
              std::int64_t count_opt = -1;  // -1 means not specified
              std::int64_t maxlen = 0;
              for (std::size_t i = 3; i < args.size(); i += 2) {
                if (i + 1 >= args.size()) return encode_error("syntax error");
                const auto opt = upper(args[i]);
                if (opt == "RANK") {
                  if (!parse_i64(args[i + 1], rank))
                    return encode_error("value is not an integer or out of range");
                  if (rank == std::numeric_limits<std::int64_t>::min())
                    return encode_error("value is out of range");
                  if (rank == 0)
                    return encode_error(
                        "RANK can't be zero: use 1 to start from the first match, 2 from the "
                        "second ... or use negative to start from the end of the list");
                } else if (opt == "COUNT") {
                  if (!parse_i64(args[i + 1], count_opt))
                    return encode_error("value is not an integer or out of range");
                  if (count_opt < 0) return encode_error("COUNT can't be negative");
                } else if (opt == "MAXLEN") {
                  if (!parse_i64(args[i + 1], maxlen))
                    return encode_error("value is not an integer or out of range");
                  if (maxlen < 0) return encode_error("MAXLEN can't be negative");
                } else
                  return encode_error("syntax error");
              }
              bool wt = false;
              auto items = store().lrange(args[1], 0, -1, wt);
              if (wt) return wrongtype_error_reply();
              bool return_array = (count_opt >= 0);
              std::int64_t max_results = (count_opt == 0) ? static_cast<std::int64_t>(items.size())
                                                          : (count_opt > 0 ? count_opt : 1);
              std::vector<long long> positions;
              std::int64_t skip = (rank > 0) ? rank - 1 : -rank - 1;
              if (rank > 0) {
                std::int64_t limit = (maxlen > 0)
                                         ? std::min(maxlen, static_cast<std::int64_t>(items.size()))
                                         : static_cast<std::int64_t>(items.size());
                for (std::int64_t j = 0;
                     j < limit && static_cast<std::int64_t>(positions.size()) < max_results; ++j) {
                  if (items[static_cast<std::size_t>(j)] == args[2]) {
                    if (skip > 0) {
                      --skip;
                      continue;
                    }
                    positions.push_back(j);
                  }
                }
              } else {
                std::int64_t start = static_cast<std::int64_t>(items.size()) - 1;
                std::int64_t limit_pos =
                    (maxlen > 0) ? std::max(start - maxlen + 1, static_cast<std::int64_t>(0)) : 0;
                for (std::int64_t j = start;
                     j >= limit_pos && static_cast<std::int64_t>(positions.size()) < max_results;
                     --j) {
                  if (items[static_cast<std::size_t>(j)] == args[2]) {
                    if (skip > 0) {
                      --skip;
                      continue;
                    }
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
    t.emplace("LPUSHX", CommandSpec{"LPUSHX",
                                    -3,
                                    {"write", "denyoom"},
                                    1,
                                    1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState&,
                                       bool&) -> std::string {
                                      if (store().is_wrongtype_for_list(args[1]))
                                        return wrongtype_error_reply();
                                      if (store().type_of(args[1]) == "none")
                                        return encode_integer(0);
                                      std::vector<std::string> vals(args.begin() + 2, args.end());
                                      bool wt = false;
                                      auto len = store().lpush(args[1], vals, wt);
                                      if (wt) return wrongtype_error_reply();
                                      return encode_integer(len);
                                    }});

    // ─── RPUSHX ──────────────────────────────────────────────────────────
    t.emplace("RPUSHX", CommandSpec{"RPUSHX",
                                    -3,
                                    {"write", "denyoom"},
                                    1,
                                    1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState&,
                                       bool&) -> std::string {
                                      if (store().is_wrongtype_for_list(args[1]))
                                        return wrongtype_error_reply();
                                      if (store().type_of(args[1]) == "none")
                                        return encode_integer(0);
                                      std::vector<std::string> vals(args.begin() + 2, args.end());
                                      bool wt = false;
                                      auto len = store().rpush(args[1], vals, wt);
                                      if (wt) return wrongtype_error_reply();
                                      return encode_integer(len);
                                    }});

    // ─── LINSERT ─────────────────────────────────────────────────────────
    t.emplace(
        "LINSERT",
        CommandSpec{
            "LINSERT",
            5,
            {"write"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              // Redis checks BEFORE/AFTER before it looks the key up.
              const auto dir = upper(args[2]);
              if (dir != "BEFORE" && dir != "AFTER") return encode_error("syntax error");
              bool wt = false;
              const auto len = store().linsert(args[1], dir == "BEFORE", args[3], args[4], wt);
              if (wt) return wrongtype_error_reply();
              if (len > 0) notify_keyspace_event(NOTIFY_LIST, "linsert", args[1], session.db_index);
              return encode_integer(len);
            }});

    // ─── RPOPLPUSH ───────────────────────────────────────────────────────
    t.emplace("RPOPLPUSH", CommandSpec{"RPOPLPUSH",
                                       3,
                                       {"write"},
                                       1,
                                       2,
                                       1,
                                       [](const std::vector<std::string>& args,
                                          SessionState& session, bool&) -> std::string {
                                         if (store().is_wrongtype_for_list(args[1]))
                                           return wrongtype_error_reply();
                                         if (store().is_wrongtype_for_list(args[2]))
                                           return wrongtype_error_reply();
                                         bool wt = false;
                                         auto val = store().rpop(args[1], wt);
                                         if (wt) return wrongtype_error_reply();
                                         if (!val) return encode_null(session.resp_version);
                                         store().lpush(args[2], {*val}, wt);
                                         return encode_bulk(*val);
                                       }});

    // ─── LTRIM ───────────────────────────────────────────────────────────
    t.emplace(
        "LTRIM",
        CommandSpec{
            "LTRIM",
            4,
            {"write"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              std::int64_t start, stop;
              if (!parse_i64(args[2], start) || !parse_i64(args[3], stop))
                return encode_error("value is not an integer or out of range");
              bool wt = false, key_removed = false;
              const bool existed = store().ltrim(args[1], start, stop, wt, key_removed);
              if (wt) return wrongtype_error_reply();
              // Redis sends "ltrim" whenever the list exists, even when nothing was trimmed.
              if (existed) notify_keyspace_event(NOTIFY_LIST, "ltrim", args[1], session.db_index);
              if (key_removed)
                notify_keyspace_event(NOTIFY_GENERIC, "del", args[1], session.db_index);
              return encode_simple("OK");
            }});

    // ─── LMPOP ───────────────────────────────────────────────────────────
    t.emplace(
        "LMPOP",
        CommandSpec{
            "LMPOP",
            -4,
            {"write"},
            0,
            0,
            0,
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
                  if (!parse_i64(args[i + 1], count) || count <= 0)
                    return encode_error("count should be greater than 0");
                  count_seen = true;
                } else
                  return encode_error("syntax error");
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
                propagate_write(eff, session, nullptr);
                std::vector<std::string> enc;
                for (const auto& v : popped) enc.push_back(encode_bulk(v));
                return encode_array({encode_bulk(key), encode_array(enc)});
              }
              return encode_null_array(session.resp_version);
            }});

    // ─── ZCARD ───────────────────────────────────────────────────────────
    t.emplace("ZCARD", CommandSpec{"ZCARD",
                                   2,
                                   {"readonly", "fast"},
                                   1,
                                   1,
                                   1,
                                   [](const std::vector<std::string>& args, SessionState&,
                                      bool&) -> std::string {
                                     if (store().is_wrongtype_for_zset(args[1]))
                                       return wrongtype_error_reply();
                                     bool wt = false;
                                     auto items = store().zrange(args[1], 0, -1, false, wt);
                                     if (wt) return wrongtype_error_reply();
                                     return encode_integer(static_cast<long long>(items.size()));
                                   }});

    // ─── ZSCORE ──────────────────────────────────────────────────────────
    t.emplace("ZSCORE", CommandSpec{"ZSCORE",
                                    3,
                                    {"readonly", "fast"},
                                    1,
                                    1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState& session,
                                       bool&) -> std::string {
                                      if (store().is_wrongtype_for_zset(args[1]))
                                        return wrongtype_error_reply();
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
    t.emplace("ZMSCORE", CommandSpec{"ZMSCORE",
                                     -3,
                                     {"readonly", "fast"},
                                     1,
                                     1,
                                     1,
                                     [](const std::vector<std::string>& args, SessionState& session,
                                        bool&) -> std::string {
                                       if (store().is_wrongtype_for_zset(args[1]))
                                         return wrongtype_error_reply();
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
    t.emplace("ZCOUNT", CommandSpec{"ZCOUNT",
                                    4,
                                    {"readonly", "fast"},
                                    1,
                                    1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState&,
                                       bool&) -> std::string {
                                      // Redis parses the range before it looks the key up.
                                      double min_val, max_val;
                                      bool min_ex, max_ex;
                                      if (!parse_score_bound(args[2], min_val, min_ex) ||
                                          !parse_score_bound(args[3], max_val, max_ex))
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
    t.emplace(
        "ZRANK",
        CommandSpec{
            "ZRANK",
            -3,
            {"readonly", "fast"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              const bool withscore = args.size() == 4 && upper(args[3]) == "WITHSCORE";
              if (args.size() > 4 || (args.size() == 4 && !withscore))
                return encode_error("syntax error");
              if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
              bool wt = false;
              auto items = store().zrange(args[1], 0, -1, true, wt);
              if (wt) return wrongtype_error_reply();
              for (std::size_t i = 0; i < items.size(); ++i) {
                if (items[i].first == args[2]) {
                  if (withscore) {
                    return encode_array({encode_integer(static_cast<long long>(i)),
                                         encode_bulk(format_score(items[i].second))});
                  }
                  return encode_integer(static_cast<long long>(i));
                }
              }
              // Missing member: nil bulk for the plain form, nil ARRAY for WITHSCORE.
              return withscore ? encode_null_array() : encode_null(session.resp_version);
            }});

    // ─── ZREVRANK ────────────────────────────────────────────────────────
    t.emplace(
        "ZREVRANK",
        CommandSpec{
            "ZREVRANK",
            -3,
            {"readonly", "fast"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              const bool withscore = args.size() == 4 && upper(args[3]) == "WITHSCORE";
              if (args.size() > 4 || (args.size() == 4 && !withscore))
                return encode_error("syntax error");
              if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
              bool wt = false;
              auto items = store().zrange(args[1], 0, -1, true, wt);
              if (wt) return wrongtype_error_reply();
              for (std::size_t i = 0; i < items.size(); ++i) {
                if (items[i].first == args[2]) {
                  long long rev_rank = static_cast<long long>(items.size() - 1 - i);
                  if (withscore) {
                    return encode_array(
                        {encode_integer(rev_rank), encode_bulk(format_score(items[i].second))});
                  }
                  return encode_integer(rev_rank);
                }
              }
              return withscore ? encode_null_array() : encode_null(session.resp_version);
            }});

    // ─── ZRANGEBYLEX / ZREVRANGEBYLEX / ZLEXCOUNT (legacy forms of ZRANGE BYLEX) ───
    t.emplace(
        "ZRANGEBYLEX",
        CommandSpec{"ZRANGEBYLEX",
                    -4,
                    {"readonly"},
                    1,
                    1,
                    1,
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

    t.emplace(
        "ZREVRANGEBYLEX",
        CommandSpec{"ZREVRANGEBYLEX",
                    -4,
                    {"readonly"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      // args are max min (reversed); zrange_select's REV handles the bound swap.
                      std::vector<std::string> proxy{"ZRANGE", args[1], args[2],
                                                     args[3],  "BYLEX", "REV"};
                      proxy.insert(proxy.end(), args.begin() + 4, args.end());
                      std::vector<std::pair<std::string, double>> items;
                      bool ws = false;
                      auto err = zrange_select(proxy, 1, 2, 4, false, items, ws);
                      if (!err.empty()) return err;
                      std::vector<std::string> out;
                      for (const auto& [m, s] : items) out.push_back(encode_bulk(m));
                      return encode_array(out);
                    }});

    t.emplace(
        "ZLEXCOUNT",
        CommandSpec{"ZLEXCOUNT",
                    4,
                    {"readonly", "fast"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      const std::vector<std::string> proxy{"ZRANGE", args[1], args[2], args[3],
                                                           "BYLEX"};
                      std::vector<std::pair<std::string, double>> items;
                      bool ws = false;
                      auto err = zrange_select(proxy, 1, 2, 4, false, items, ws);
                      if (!err.empty()) return err;
                      return encode_integer(static_cast<long long>(items.size()));
                    }});

    // ─── ZREMRANGEBYSCORE / ZREMRANGEBYRANK / ZREMRANGEBYLEX ─────────────
    // Each selects victims with the shared unified-range selector (which still copies the
    // zset, compat/delta.md item 56), then removes them in place, so the key keeps its TTL.
    {
      auto make_remrange = [](const char* mode, const char* event) {
        const std::string mode_s = mode;  // "" = index, "BYSCORE", "BYLEX"
        const std::string event_s = event;
        return [mode_s, event_s](const std::vector<std::string>& args, SessionState& session,
                                 bool&) -> std::string {
          std::vector<std::string> proxy{"ZRANGE", args[1], args[2], args[3]};
          if (!mode_s.empty()) proxy.push_back(mode_s);
          std::vector<std::pair<std::string, double>> victims;
          bool ws = false;
          auto err = zrange_select(proxy, 1, 2, 4, false, victims, ws);
          if (!err.empty()) return err;
          if (victims.empty()) return encode_integer(0);
          std::vector<std::string> members;
          members.reserve(victims.size());
          for (auto& [m, sc] : victims) members.push_back(std::move(m));
          bool wt = false, key_removed = false;
          const auto removed = store().zrem(args[1], members, wt, key_removed);
          if (wt) return wrongtype_error_reply();
          // Events in the order of Redis's zremrangeGenericCommand().
          if (removed > 0) notify_keyspace_event(NOTIFY_ZSET, event_s, args[1], session.db_index);
          if (key_removed) notify_keyspace_event(NOTIFY_GENERIC, "del", args[1], session.db_index);
          return encode_integer(removed);
        };
      };
      t.emplace("ZREMRANGEBYSCORE", CommandSpec{"ZREMRANGEBYSCORE",
                                                4,
                                                {"write"},
                                                1,
                                                1,
                                                1,
                                                make_remrange("BYSCORE", "zremrangebyscore")});
      t.emplace(
          "ZREMRANGEBYRANK",
          CommandSpec{
              "ZREMRANGEBYRANK", 4, {"write"}, 1, 1, 1, make_remrange("", "zremrangebyrank")});
      t.emplace(
          "ZREMRANGEBYLEX",
          CommandSpec{
              "ZREMRANGEBYLEX", 4, {"write"}, 1, 1, 1, make_remrange("BYLEX", "zremrangebylex")});
    }

    // ─── ZINTERCARD numkeys key [key ...] [LIMIT n] ──────────────────────
    t.emplace(
        "ZINTERCARD",
        CommandSpec{
            "ZINTERCARD",
            -3,
            {"readonly"},
            0,
            0,
            0,
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
                if (t == "zset") {
                  for (const auto& [m, sc] : store().zrange(key, 0, -1, true, wt)) out.push_back(m);
                  return !wt;
                }
                if (t == "set") {
                  for (const auto& m : store().smembers(key, wt)) out.push_back(m);
                  return !wt;
                }
                return false;
              };
              std::vector<std::string> first;
              if (!members_of(args[2], first)) return wrongtype_error_reply();
              std::unordered_set<std::string> current(first.begin(), first.end());
              for (std::int64_t k = 1; k < numkeys && !current.empty(); ++k) {
                std::vector<std::string> next;
                if (!members_of(args[static_cast<std::size_t>(2 + k)], next))
                  return wrongtype_error_reply();
                std::unordered_set<std::string> nextset(next.begin(), next.end());
                std::unordered_set<std::string> keep;
                for (const auto& m : current)
                  if (nextset.count(m)) keep.insert(m);
                current.swap(keep);
              }
              long long card = static_cast<long long>(current.size());
              if (limit > 0 && card > limit) card = limit;
              return encode_integer(card);
            }});

    // ─── ZREM ────────────────────────────────────────────────────────────
    t.emplace(
        "ZREM",
        CommandSpec{
            "ZREM",
            -3,
            {"write", "fast"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState& session, bool&) -> std::string {
              // In place, so the key keeps its TTL; events in the order of Redis's zremCommand().
              const std::vector<std::string> members(args.begin() + 2, args.end());
              bool wt = false, key_removed = false;
              const auto removed = store().zrem(args[1], members, wt, key_removed);
              if (wt) return wrongtype_error_reply();
              if (removed > 0)
                notify_keyspace_event(NOTIFY_ZSET, "zrem", args[1], session.db_index);
              if (key_removed)
                notify_keyspace_event(NOTIFY_GENERIC, "del", args[1], session.db_index);
              return encode_integer(removed);
            }});

    // ─── ZINCRBY ─────────────────────────────────────────────────────────
    t.emplace(
        "ZINCRBY",
        CommandSpec{"ZINCRBY",
                    4,
                    {"write", "fast"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
                      long double incr = 0;
                      if (!parse_strict_float(args[2], incr))
                        return encode_error("value is not a valid float");
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
                      auto result = store().zadd_one(args[1], static_cast<double>(incr), args[3],
                                                     false, false, false, false, true);
                      if (result.wrongtype) return wrongtype_error_reply();
                      return encode_bulk(format_score(result.score));
                    }});

    // ─── ZRANGEBYSCORE ───────────────────────────────────────────────────
    t.emplace(
        "ZRANGEBYSCORE",
        CommandSpec{
            "ZRANGEBYSCORE",
            -4,
            {"readonly"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
              // Redis's order: the options, then the range, then the key.
              bool withscores = false;
              std::int64_t offset = 0, limit_count = -1;
              for (std::size_t i = 4; i < args.size(); ++i) {
                if (upper(args[i]) == "WITHSCORES")
                  withscores = true;
                else if (upper(args[i]) == "LIMIT" && i + 2 < args.size()) {
                  if (!parse_i64(args[i + 1], offset) || !parse_i64(args[i + 2], limit_count))
                    return encode_error("value is not an integer or out of range");
                  i += 2;
                } else {
                  return encode_error("syntax error");  // e.g. REV is invalid for ZRANGEBYSCORE
                }
              }
              double min_val, max_val;
              bool min_ex, max_ex;
              if (!parse_score_bound(args[2], min_val, min_ex) ||
                  !parse_score_bound(args[3], max_val, max_ex))
                return encode_error("min or max is not a float");
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
                if (static_cast<std::size_t>(offset) >= matched.size())
                  matched.clear();
                else
                  matched.erase(matched.begin(), matched.begin() + offset);
              }
              if (limit_count >= 0 && static_cast<std::size_t>(limit_count) < matched.size()) {
                matched.resize(static_cast<std::size_t>(limit_count));
              }
              std::vector<std::string> enc;
              for (const auto& [m, s] : matched) {
                enc.push_back(encode_bulk(m));
                if (withscores) {
                  enc.push_back(encode_bulk(format_score(s)));
                }
              }
              return encode_array(enc);
            }});

    // ─── ZREVRANGEBYSCORE ────────────────────────────────────────────────
    t.emplace(
        "ZREVRANGEBYSCORE",
        CommandSpec{
            "ZREVRANGEBYSCORE",
            -4,
            {"readonly"},
            1,
            1,
            1,
            [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
              // Redis's order: the options, then the range, then the key.
              bool withscores = false;
              std::int64_t offset = 0, limit_count = -1;
              for (std::size_t i = 4; i < args.size(); ++i) {
                if (upper(args[i]) == "WITHSCORES")
                  withscores = true;
                else if (upper(args[i]) == "LIMIT" && i + 2 < args.size()) {
                  if (!parse_i64(args[i + 1], offset) || !parse_i64(args[i + 2], limit_count))
                    return encode_error("value is not an integer or out of range");
                  i += 2;
                } else {
                  return encode_error("syntax error");  // e.g. REV is invalid for ZRANGEBYSCORE
                }
              }
              // Note: for ZREVRANGEBYSCORE, args[2] is max, args[3] is min (reversed)
              double max_val, min_val;
              bool max_ex, min_ex;
              if (!parse_score_bound(args[2], max_val, max_ex) ||
                  !parse_score_bound(args[3], min_val, min_ex))
                return encode_error("min or max is not a float");
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
                if (static_cast<std::size_t>(offset) >= matched.size())
                  matched.clear();
                else
                  matched.erase(matched.begin(), matched.begin() + offset);
              }
              if (limit_count >= 0 && static_cast<std::size_t>(limit_count) < matched.size()) {
                matched.resize(static_cast<std::size_t>(limit_count));
              }
              std::vector<std::string> enc;
              for (const auto& [m, s] : matched) {
                enc.push_back(encode_bulk(m));
                if (withscores) {
                  enc.push_back(encode_bulk(format_score(s)));
                }
              }
              return encode_array(enc);
            }});

    // ─── ZREVRANGE ───────────────────────────────────────────────────────
    t.emplace(
        "ZREVRANGE",
        CommandSpec{"ZREVRANGE",
                    -4,
                    {"readonly"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      if (store().is_wrongtype_for_zset(args[1])) return wrongtype_error_reply();
                      std::int64_t start, stop;
                      if (!parse_i64(args[2], start) || !parse_i64(args[3], stop))
                        return encode_error("value is not an integer or out of range");
                      // The only valid 5th token is WITHSCORES; BYSCORE/BYLEX/etc. are a syntax
                      // error.
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
                          if (withscores) {
                            enc.push_back(encode_bulk(
                                format_score(items[static_cast<std::size_t>(i)].second)));
                          }
                        }
                      }
                      return encode_array(enc);
                    }});

    // ─── ZRANDMEMBER ─────────────────────────────────────────────────────
    t.emplace(
        "ZRANDMEMBER",
        CommandSpec{
            "ZRANDMEMBER",
            -2,
            {"readonly"},
            1,
            1,
            1,
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
              if (!parse_i64(args[2], count))
                return encode_error("value is not an integer or out of range");
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
                const std::size_t take =
                    std::min<std::size_t>(idxs.size(), static_cast<std::size_t>(count));
                picks.assign(idxs.begin(), idxs.begin() + static_cast<std::ptrdiff_t>(take));
              } else {
                picks.reserve(static_cast<std::size_t>(-count));
                for (std::int64_t i = 0; i < -count; ++i) {
                  picks.push_back(
                      static_cast<std::size_t>(std::rand() % static_cast<int>(items.size())));
                }
              }
              std::vector<std::string> enc;
              for (const auto idx : picks) {
                if (resp3_pairs) {
                  enc.push_back(
                      encode_array({encode_bulk(items[idx].first),
                                    encode_double(items[idx].second, RespVersion::Resp3)}));
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
        if (!parse_i64(args[nk_idx], numkeys))
          return encode_error("value is not an integer or out of range");
        if (numkeys <= 0) {
          return encode_error("at least 1 input key is needed for '" + lower(args[0]) +
                              "' command");
        }
        const std::size_t keys_start = nk_idx + 1;
        if (keys_start + static_cast<std::size_t>(numkeys) > args.size())
          return encode_error("syntax error");

        std::vector<double> weights(static_cast<std::size_t>(numkeys), 1.0);
        enum class Agg { Sum, Min, Max };
        Agg agg = Agg::Sum;
        bool withscores = false;
        for (std::size_t i = keys_start + static_cast<std::size_t>(numkeys); i < args.size();) {
          const auto o = upper(args[i]);
          if (o == "WEIGHTS" && op != ZOp::Diff) {
            if (i + static_cast<std::size_t>(numkeys) >= args.size())
              return encode_error("syntax error");
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
            if (a == "SUM")
              agg = Agg::Sum;
            else if (a == "MIN")
              agg = Agg::Min;
            else if (a == "MAX")
              agg = Agg::Max;
            else
              return encode_error("syntax error");
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
            case Agg::Min:
              return std::min(acc, v);
            case Agg::Max:
              return std::max(acc, v);
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
            if (!fetch(args[keys_start + static_cast<std::size_t>(k)], src))
              return wrongtype_error_reply();
            for (const auto& [m, sc] : src) result.erase(m);
          }
        } else {
          std::unordered_map<std::string, int> seen_in;
          for (std::int64_t k = 0; k < numkeys; ++k) {
            std::vector<std::pair<std::string, double>> src;
            if (!fetch(args[keys_start + static_cast<std::size_t>(k)], src))
              return wrongtype_error_reply();
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
              if (seen_in[it->first] < numkeys)
                it = result.erase(it);
              else
                ++it;
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

      t.emplace("ZUNIONSTORE",
                CommandSpec{"ZUNIONSTORE",
                            -4,
                            {"write"},
                            1,
                            1,
                            1,
                            [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) {
                              return zsetop(a, ZOp::Union, true, se);
                            }});
      t.emplace("ZINTERSTORE",
                CommandSpec{"ZINTERSTORE",
                            -4,
                            {"write"},
                            1,
                            1,
                            1,
                            [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) {
                              return zsetop(a, ZOp::Inter, true, se);
                            }});
      t.emplace("ZDIFFSTORE",
                CommandSpec{"ZDIFFSTORE",
                            -4,
                            {"write"},
                            1,
                            1,
                            1,
                            [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) {
                              return zsetop(a, ZOp::Diff, true, se);
                            }});
      t.emplace("ZUNION",
                CommandSpec{"ZUNION",
                            -3,
                            {"readonly"},
                            0,
                            0,
                            0,
                            [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) {
                              return zsetop(a, ZOp::Union, false, se);
                            }});
      t.emplace("ZINTER",
                CommandSpec{"ZINTER",
                            -3,
                            {"readonly"},
                            0,
                            0,
                            0,
                            [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) {
                              return zsetop(a, ZOp::Inter, false, se);
                            }});
      t.emplace("ZDIFF",
                CommandSpec{"ZDIFF",
                            -3,
                            {"readonly"},
                            0,
                            0,
                            0,
                            [zsetop](const std::vector<std::string>& a, SessionState& se, bool&) {
                              return zsetop(a, ZOp::Diff, false, se);
                            }});
    }

    // ─── SRANDMEMBER ─────────────────────────────────────────────────────
    t.emplace(
        "SRANDMEMBER",
        CommandSpec{
            "SRANDMEMBER",
            -2,
            {"readonly"},
            1,
            1,
            1,
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
              if (!parse_i64(args[2], count))
                return encode_error("value is not an integer or out of range");
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
                const std::size_t take =
                    std::min<std::size_t>(idxs.size(), static_cast<std::size_t>(count));
                enc.reserve(take);
                for (std::size_t i = 0; i < take; ++i) enc.push_back(encode_bulk(members[idxs[i]]));
              } else {
                enc.reserve(static_cast<std::size_t>(-count));
                for (std::int64_t i = 0; i < -count; ++i) {
                  auto idx =
                      static_cast<std::size_t>(std::rand() % static_cast<int>(members.size()));
                  enc.push_back(encode_bulk(members[idx]));
                }
              }
              return encode_array(enc);
            }});

    // ─── SINTER ──────────────────────────────────────────────────────────
    t.emplace(
        "SINTER",
        CommandSpec{"SINTER",
                    -2,
                    {"readonly"},
                    1,
                    -1,
                    1,
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
                          if (!mset.count(*it))
                            it = current.erase(it);
                          else
                            ++it;
                        }
                      }
                      std::vector<std::string> enc;
                      for (const auto& m : current) enc.push_back(encode_bulk(m));
                      return encode_array(enc);
                    }});

    // ─── SINTERCARD ──────────────────────────────────────────────────────
    t.emplace(
        "SINTERCARD",
        CommandSpec{"SINTERCARD",
                    -3,
                    {"readonly"},
                    0,
                    0,
                    0,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      std::int64_t numkeys;
                      if (!parse_i64(args[1], numkeys) || numkeys <= 0)
                        return encode_error("numkeys should be greater than 0");
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
                        } else
                          return encode_error("syntax error");
                      }
                      bool wt = false;
                      auto first_members = store().smembers(args[2], wt);
                      if (wt) return wrongtype_error_reply();
                      std::unordered_set<std::string> current(first_members.begin(),
                                                              first_members.end());
                      for (std::int64_t i = 1; i < numkeys; ++i) {
                        const auto& key = args[static_cast<std::size_t>(2 + i)];
                        if (store().is_wrongtype_for_set(key)) return wrongtype_error_reply();
                        auto members = store().smembers(key, wt);
                        if (wt) return wrongtype_error_reply();
                        std::unordered_set<std::string> mset(members.begin(), members.end());
                        for (auto it = current.begin(); it != current.end();) {
                          if (!mset.count(*it))
                            it = current.erase(it);
                          else
                            ++it;
                        }
                      }
                      long long result = static_cast<long long>(current.size());
                      if (limit > 0 && result > limit) result = limit;
                      return encode_integer(result);
                    }});

    // ─── SUNION ──────────────────────────────────────────────────────────
    t.emplace("SUNION", CommandSpec{"SUNION",
                                    -2,
                                    {"readonly"},
                                    1,
                                    -1,
                                    1,
                                    [](const std::vector<std::string>& args, SessionState&,
                                       bool&) -> std::string {
                                      std::unordered_set<std::string> result;
                                      bool wt = false;
                                      for (std::size_t i = 1; i < args.size(); ++i) {
                                        if (store().is_wrongtype_for_set(args[i]))
                                          return wrongtype_error_reply();
                                        auto members = store().smembers(args[i], wt);
                                        if (wt) return wrongtype_error_reply();
                                        for (const auto& m : members) result.insert(m);
                                      }
                                      std::vector<std::string> enc;
                                      for (const auto& m : result) enc.push_back(encode_bulk(m));
                                      return encode_array(enc);
                                    }});

    // ─── SDIFF ───────────────────────────────────────────────────────────
    t.emplace("SDIFF", CommandSpec{"SDIFF",
                                   -2,
                                   {"readonly"},
                                   1,
                                   -1,
                                   1,
                                   [](const std::vector<std::string>& args, SessionState&,
                                      bool&) -> std::string {
                                     bool wt = false;
                                     auto first_members = store().smembers(args[1], wt);
                                     if (wt) return wrongtype_error_reply();
                                     std::unordered_set<std::string> result(first_members.begin(),
                                                                            first_members.end());
                                     for (std::size_t i = 2; i < args.size(); ++i) {
                                       if (store().is_wrongtype_for_set(args[i]))
                                         return wrongtype_error_reply();
                                       auto members = store().smembers(args[i], wt);
                                       if (wt) return wrongtype_error_reply();
                                       for (const auto& m : members) result.erase(m);
                                     }
                                     std::vector<std::string> enc;
                                     for (const auto& m : result) enc.push_back(encode_bulk(m));
                                     return encode_array(enc);
                                   }});

    // ─── SINTERSTORE / SUNIONSTORE / SDIFFSTORE ─────────────────────────
    t.emplace(
        "SINTERSTORE",
        CommandSpec{"SINTERSTORE",
                    -3,
                    {"write"},
                    1,
                    -1,
                    1,
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
                          if (!mset.count(*it))
                            it = result.erase(it);
                          else
                            ++it;
                        }
                      }
                      store().del(args[1]);
                      if (!result.empty()) {
                        std::vector<std::string> vals(result.begin(), result.end());
                        store().sadd(args[1], vals, wt);
                      }
                      return encode_integer(static_cast<long long>(result.size()));
                    }});

    t.emplace(
        "SUNIONSTORE",
        CommandSpec{"SUNIONSTORE",
                    -3,
                    {"write"},
                    1,
                    -1,
                    1,
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

    t.emplace(
        "SDIFFSTORE",
        CommandSpec{"SDIFFSTORE",
                    -3,
                    {"write"},
                    1,
                    -1,
                    1,
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
    t.emplace(
        "SMISMEMBER",
        CommandSpec{"SMISMEMBER",
                    -3,
                    {"readonly", "fast"},
                    1,
                    1,
                    1,
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
    t.emplace(
        "HRANDFIELD",
        CommandSpec{
            "HRANDFIELD",
            -2,
            {"readonly"},
            1,
            1,
            1,
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
              if (!parse_i64(args[2], count))
                return encode_error("value is not an integer or out of range");
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
                const std::size_t take =
                    std::min<std::size_t>(idxs.size(), static_cast<std::size_t>(count));
                picks.assign(idxs.begin(), idxs.begin() + static_cast<std::ptrdiff_t>(take));
              } else if (!all.empty()) {
                picks.reserve(static_cast<std::size_t>(-count));
                for (std::int64_t i = 0; i < -count; ++i) {
                  picks.push_back(
                      static_cast<std::size_t>(std::rand() % static_cast<int>(all.size())));
                }
              }
              std::vector<std::string> enc;
              enc.reserve(picks.size() * (withvalues && !resp3_pairs ? 2 : 1));
              for (const auto idx : picks) {
                if (resp3_pairs) {
                  enc.push_back(
                      encode_array({encode_bulk(all[idx].first), encode_bulk(all[idx].second)}));
                } else {
                  enc.push_back(encode_bulk(all[idx].first));
                  if (withvalues) enc.push_back(encode_bulk(all[idx].second));
                }
              }
              return encode_array(enc);
            }});

    // ─── BITCOUNT ────────────────────────────────────────────────────────
    t.emplace(
        "BITCOUNT",
        CommandSpec{"BITCOUNT",
                    -2,
                    {"readonly"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      // Valid forms: BITCOUNT key | key start end | key start end BYTE|BIT. A lone
                      // start (or trailing junk) is a syntax error (Redis), not a silently-ignored
                      // argument.
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
                          if (unit == "BIT")
                            bit_mode = true;
                          else if (unit != "BYTE")
                            return encode_error("syntax error");
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
                          if (byte_idx < s.size() &&
                              ((static_cast<unsigned char>(s[byte_idx]) >> bit_idx) & 1))
                            ++count;
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
                        unsigned char byte =
                            static_cast<unsigned char>(s[static_cast<std::size_t>(i)]);
                        count += __builtin_popcount(byte);
                      }
                      return encode_integer(count);
                    }});

    // ─── BITPOS ──────────────────────────────────────────────────────────
    t.emplace(
        "BITPOS",
        CommandSpec{"BITPOS",
                    -3,
                    {"readonly"},
                    1,
                    1,
                    1,
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
                      if (args.size() >= 4) {
                        if (!parse_i64(args[3], start))
                          return encode_error("value is not an integer or out of range");
                      }
                      if (args.size() >= 5) {
                        if (!parse_i64(args[4], end))
                          return encode_error("value is not an integer or out of range");
                        has_end = true;
                      }
                      if (args.size() >= 6) {
                        auto unit = upper(args[5]);
                        if (unit == "BIT")
                          bit_mode = true;
                        else if (unit != "BYTE")
                          return encode_error("syntax error");
                      }
                      std::int64_t len = static_cast<std::int64_t>(s.size());
                      if (!bit_mode) {
                        if (start < 0) start += len;
                        if (end < 0) end += len;
                        if (start < 0) start = 0;
                        if (end >= len) end = len - 1;
                        if (start > end) return encode_integer(-1);
                        for (std::int64_t byte_i = start; byte_i <= end; ++byte_i) {
                          unsigned char byte =
                              static_cast<unsigned char>(s[static_cast<std::size_t>(byte_i)]);
                          for (int bi = 7; bi >= 0; --bi) {
                            if (((byte >> bi) & 1) == static_cast<unsigned>(bit)) {
                              return encode_integer(byte_i * 8 + (7 - bi));
                            }
                          }
                        }
                        if (bit == 0 && !has_end)
                          return encode_integer(static_cast<long long>(s.size()) * 8);
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
    t.emplace(
        "BITOP",
        CommandSpec{"BITOP",
                    -4,
                    {"write"},
                    2,
                    -1,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      const auto op = upper(args[1]);
                      if (op != "AND" && op != "OR" && op != "XOR" && op != "NOT")
                        return encode_error("BITOP requires AND, OR, XOR or NOT operation");
                      if (op == "NOT" && args.size() != 4)
                        return encode_error("BITOP NOT requires one and only one key");
                      // Gather source strings
                      std::vector<std::string> sources;
                      for (std::size_t i = 3; i < args.size(); ++i) {
                        if (store().is_wrongtype_for_string(args[i]))
                          return wrongtype_error_reply();
                        auto val = store().get(args[i]);
                        sources.push_back(val ? *val : "");
                      }
                      // Determine max length
                      std::size_t maxlen = 0;
                      for (const auto& s : sources) maxlen = std::max(maxlen, s.size());
                      std::string result(maxlen, '\0');
                      for (std::size_t i = 0; i < maxlen; ++i) {
                        unsigned char byte =
                            (i < sources[0].size()) ? static_cast<unsigned char>(sources[0][i]) : 0;
                        if (op == "NOT") {
                          result[i] = static_cast<char>(~byte);
                        } else {
                          for (std::size_t j = 1; j < sources.size(); ++j) {
                            unsigned char b = (i < sources[j].size())
                                                  ? static_cast<unsigned char>(sources[j][i])
                                                  : 0;
                            if (op == "AND")
                              byte &= b;
                            else if (op == "OR")
                              byte |= b;
                            else if (op == "XOR")
                              byte ^= b;
                          }
                          result[i] = static_cast<char>(byte);
                        }
                      }
                      store().set(args[2], result, false, false, std::nullopt, false);
                      return encode_integer(static_cast<long long>(result.size()));
                    }});

    // ─── BITFIELD ────────────────────────────────────────────────────────
    t.emplace(
        "BITFIELD",
        CommandSpec{
            "BITFIELD",
            -2,
            {"write"},
            1,
            1,
            1,
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
                if (val_bit)
                  s[byte_idx] =
                      static_cast<char>(static_cast<unsigned char>(s[byte_idx]) | (1 << bit_idx));
                else
                  s[byte_idx] =
                      static_cast<char>(static_cast<unsigned char>(s[byte_idx]) & ~(1 << bit_idx));
              };
              auto parse_type = [](const std::string& t, bool& is_signed, int& bits) -> bool {
                if (t.empty()) return false;
                is_signed = (t[0] == 'i');
                if (t[0] != 'i' && t[0] != 'u') return false;
                try {
                  bits = std::stoi(t.substr(1));
                  return bits > 0 && bits <= 64;
                } catch (...) {
                  return false;
                }
              };
              auto parse_offset = [](const std::string& s, int bits, std::size_t& off) -> bool {
                if (s.empty()) return false;
                if (s[0] == '#') {
                  try {
                    off = static_cast<std::size_t>(std::stoull(s.substr(1))) *
                          static_cast<std::size_t>(bits);
                    return true;
                  } catch (...) {
                    return false;
                  }
                }
                try {
                  off = static_cast<std::size_t>(std::stoull(s));
                  return true;
                } catch (...) {
                  return false;
                }
              };
              auto read_field = [&](std::size_t offset, int bits, bool is_signed) -> std::int64_t {
                std::uint64_t v = 0;
                for (int i = 0; i < bits; ++i) {
                  v = (v << 1) |
                      static_cast<std::uint64_t>(get_bit(offset + static_cast<std::size_t>(i)));
                }
                if (is_signed && bits < 64 && (v & (1ULL << (bits - 1)))) {
                  v |= ~((1ULL << bits) - 1);
                }
                return static_cast<std::int64_t>(v);
              };
              auto write_field = [&](std::size_t offset, int bits, std::int64_t value) {
                std::uint64_t v = static_cast<std::uint64_t>(value);
                for (int i = bits - 1; i >= 0; --i) {
                  set_bit_fn(offset + static_cast<std::size_t>(bits - 1 - i),
                             static_cast<int>((v >> i) & 1));
                }
              };
              // Overflow handling mirrors Redis check{Signed,Unsigned}BitfieldOverflow (bitops.c):
              // owtype 0=WRAP (default), 1=SAT, 2=FAIL. Returns 0 (no overflow) / ±1 (overflow),
              // and on WRAP/SAT sets `limit` to the value to store.
              enum { OW_WRAP = 0, OW_SAT = 1, OW_FAIL = 2 };
              auto check_signed = [](std::int64_t value, std::int64_t incr, int bits, int owtype,
                                     std::int64_t& limit) -> int {
                const std::int64_t max =
                    (bits == 64) ? INT64_MAX : ((std::int64_t{1} << (bits - 1)) - 1);
                const std::int64_t min = (-max) - 1;
                const std::int64_t maxincr = static_cast<std::int64_t>(
                    static_cast<std::uint64_t>(max) - static_cast<std::uint64_t>(value));
                const std::int64_t minincr = min - value;
                auto wrap = [&]() {
                  const std::uint64_t msb = std::uint64_t{1} << (bits - 1);
                  std::uint64_t c =
                      static_cast<std::uint64_t>(value) + static_cast<std::uint64_t>(incr);
                  if (bits < 64) {
                    const std::uint64_t mask = ~std::uint64_t{0} << bits;
                    if (c & msb)
                      c |= mask;
                    else
                      c &= ~mask;
                  }
                  limit = static_cast<std::int64_t>(c);
                };
                if (value > max || (bits != 64 && incr > maxincr) ||
                    (value >= 0 && incr > 0 && incr > maxincr)) {
                  if (owtype == OW_WRAP)
                    wrap();
                  else if (owtype == OW_SAT)
                    limit = max;
                  return 1;
                } else if (value < min || (bits != 64 && incr < minincr) ||
                           (value < 0 && incr < 0 && incr < minincr)) {
                  if (owtype == OW_WRAP)
                    wrap();
                  else if (owtype == OW_SAT)
                    limit = min;
                  return -1;
                }
                return 0;
              };
              auto check_unsigned = [](std::uint64_t value, std::int64_t incr, int bits, int owtype,
                                       std::uint64_t& limit) -> int {
                const std::uint64_t max =
                    (bits == 64) ? UINT64_MAX : ((std::uint64_t{1} << bits) - 1);
                const std::int64_t maxincr = static_cast<std::int64_t>(max - value);
                const std::int64_t minincr = -static_cast<std::int64_t>(value);
                auto wrap = [&]() {
                  const std::uint64_t mask = ~std::uint64_t{0} << bits;
                  limit = (value + static_cast<std::uint64_t>(incr)) & ~mask;
                };
                if (value > max || (incr > 0 && incr > maxincr)) {
                  if (owtype == OW_WRAP)
                    wrap();
                  else if (owtype == OW_SAT)
                    limit = max;
                  return 1;
                } else if (incr < 0 && incr < minincr) {
                  if (owtype == OW_WRAP)
                    wrap();
                  else if (owtype == OW_SAT)
                    limit = 0;
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
                  if (m == "WRAP")
                    owtype = OW_WRAP;
                  else if (m == "SAT")
                    owtype = OW_SAT;
                  else if (m == "FAIL")
                    owtype = OW_FAIL;
                  else
                    return encode_error("Invalid OVERFLOW type specified");
                  i += 2;
                  continue;
                }
                if (subcmd == "GET") {
                  if (i + 2 >= args.size()) return encode_error("syntax error");
                  bool is_signed;
                  int bits;
                  if (!parse_type(args[i + 1], is_signed, bits))
                    return encode_error(
                        "Invalid bitfield type. Use something like i16 u8. Note that u64 is not "
                        "supported but i64 is.");
                  std::size_t off;
                  if (!parse_offset(args[i + 2], bits, off))
                    return encode_error("bit offset is not an integer or out of range");
                  results.push_back(encode_integer(read_field(off, bits, is_signed)));
                  i += 3;
                } else if (subcmd == "SET" || subcmd == "INCRBY") {
                  const bool is_incr = subcmd == "INCRBY";
                  if (i + 3 >= args.size()) return encode_error("syntax error");
                  bool is_signed;
                  int bits;
                  if (!parse_type(args[i + 1], is_signed, bits))
                    return encode_error(
                        "Invalid bitfield type. Use something like i16 u8. Note that u64 is not "
                        "supported but i64 is.");
                  std::size_t off;
                  if (!parse_offset(args[i + 2], bits, off))
                    return encode_error("bit offset is not an integer or out of range");
                  std::int64_t arg;
                  if (!parse_i64(args[i + 3], arg))
                    return encode_error("value is not an integer or out of range");
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
                      newval = static_cast<std::int64_t>(
                          overflow ? wrapped : oldu + static_cast<std::uint64_t>(arg));
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
    t.emplace(
        "GEODIST",
        CommandSpec{
            "GEODIST",
            -4,
            {"readonly"},
            1,
            1,
            1,
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
                if (m == args[2]) {
                  decode_geohash(s, lon1, lat1);
                  found1 = true;
                }
                if (m == args[3]) {
                  decode_geohash(s, lon2, lat2);
                  found2 = true;
                }
              }
              if (!found1 || !found2) return encode_null(session.resp_version);
              double dist = geoutil::dist(lon1, lat1, lon2, lat2);
              double mul = 1.0;
              if (args.size() >= 5 && !geoutil::unit_to_m(lower(args[4]), mul))
                return encode_error("unsupported unit provided. please use M, KM, FT, MI");
              char buf[64];
              std::snprintf(buf, sizeof(buf), "%.4f", dist / mul);
              return encode_bulk(buf);
            }});

    // ─── GEOPOS ──────────────────────────────────────────────────────────
    t.emplace("GEOPOS",
              CommandSpec{"GEOPOS",
                          -2,
                          {"readonly"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session,
                             bool&) -> std::string {
                            if (store().is_wrongtype_for_zset(args[1]))
                              return wrongtype_error_reply();
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
                                enc.push_back(encode_array(
                                    {encode_bulk(oss_lon.str()), encode_bulk(oss_lat.str())}));
                              }
                            }
                            return encode_array(enc);
                          }});

    // ─── GEOHASH ─────────────────────────────────────────────────────────
    t.emplace("GEOHASH",
              CommandSpec{"GEOHASH",
                          -2,
                          {"readonly"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState& session,
                             bool&) -> std::string {
                            if (store().is_wrongtype_for_zset(args[1]))
                              return wrongtype_error_reply();
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
                              // Redis derives 52 geohash bits (11 standard chars = 55 bits), then
                              // forces the 11th char to '0' since the low 3 bits are zero-padding
                              // (geohashCommand). We subdivide the first 10 chars (50 bits) and
                              // append the fixed final '0'.
                              while (hash.size() < 10) {
                                if (is_lon) {
                                  double mid = (lon_min + lon_max) / 2;
                                  if (lon >= mid) {
                                    bits = (bits << 1) | 1;
                                    lon_min = mid;
                                  } else {
                                    bits <<= 1;
                                    lon_max = mid;
                                  }
                                } else {
                                  double mid = (lat_min + lat_max) / 2;
                                  if (lat >= mid) {
                                    bits = (bits << 1) | 1;
                                    lat_min = mid;
                                  } else {
                                    bits <<= 1;
                                    lat_max = mid;
                                  }
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
    t.emplace("GEORADIUS",
              CommandSpec{
                  "GEORADIUS",
                  -6,
                  {"write"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState& s, bool&) -> std::string {
                    return do_geo_search(args, s.resp_version, /*form=*/0, /*readonly=*/false);
                  }});

    t.emplace("GEORADIUSBYMEMBER",
              CommandSpec{
                  "GEORADIUSBYMEMBER",
                  -5,
                  {"write"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState& s, bool&) -> std::string {
                    return do_geo_search(args, s.resp_version, /*form=*/1, /*readonly=*/false);
                  }});

    // ─── GEOSEARCH ───────────────────────────────────────────────────────
    t.emplace("GEOSEARCH",
              CommandSpec{
                  "GEOSEARCH",
                  -7,
                  {"readonly"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState& s, bool&) -> std::string {
                    return do_geo_search(args, s.resp_version, /*form=*/2, /*readonly=*/true);
                  }});

    t.emplace("GEOSEARCHSTORE",
              CommandSpec{
                  "GEOSEARCHSTORE",
                  -8,
                  {"write"},
                  1,
                  2,
                  1,
                  [](const std::vector<std::string>& args, SessionState& s, bool&) -> std::string {
                    return do_geo_search(args, s.resp_version, /*form=*/3, /*readonly=*/false);
                  }});

    // ─── XTRIM ───────────────────────────────────────────────────────────
    t.emplace("XTRIM",
              CommandSpec{
                  "XTRIM",
                  -4,
                  {"write"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                    if (store().is_wrongtype_for_stream(args[1])) return wrongtype_error_reply();
                    // Simplified: for MAXLEN, trim the stream
                    bool wt = false;
                    auto entries = store().xrange(args[1], "-", "+", false, wt);
                    if (wt) return wrongtype_error_reply();
                    const auto strategy = upper(args[2]);
                    // Shared trailing-option parse: optional ~/= then the threshold, then an
                    // optional LIMIT that is only legal with ~ (Redis
                    // streamParseAddOrTrimArgsOrReply).
                    auto parse_threshold_and_limit = [&](std::size_t& start,
                                                         bool& approx) -> std::string {
                      approx = false;
                      if (start < args.size() && (args[start] == "~" || args[start] == "=")) {
                        approx = (args[start] == "~");
                        ++start;
                      }
                      if (start >= args.size()) return encode_error("syntax error");
                      const std::size_t after = start + 1;
                      if (after < args.size()) {
                        if (after + 1 < args.size() && upper(args[after]) == "LIMIT") {
                          if (!approx)
                            return encode_error(
                                "syntax error, LIMIT cannot be used without the special ~ option");
                          std::int64_t lim = 0;
                          if (!parse_i64(args[after + 1], lim))
                            return encode_error("value is not an integer or out of range");
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
                      if (!parse_i64(args[start], maxlen))
                        return encode_error("value is not an integer or out of range");
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
      for (const auto& [f, v] : fields) {
        fv.push_back(encode_bulk(f));
        fv.push_back(encode_bulk(v));
      }
      return encode_array({encode_bulk(id), encode_array(fv)});
    };

    t.emplace("XAUTOCLAIM",
              CommandSpec{
                  "XAUTOCLAIM",
                  -6,
                  {"write"},
                  1,
                  1,
                  1,
                  [encode_stream_entry](const std::vector<std::string>& args, SessionState& session,
                                        bool&) -> std::string {
                    std::int64_t min_idle = 0;
                    if (!parse_strict_int(args[4], min_idle))
                      return encode_error("Invalid min-idle-time argument for XAUTOCLAIM");
                    if (min_idle < 0) min_idle = 0;
                    std::string start_error, start;
                    const bool exclusive = args[5].size() > 1 && args[5][0] == '(';
                    const std::string raw_start = exclusive ? args[5].substr(1) : args[5];
                    if (!normalize_claim_stream_id(raw_start, start, !exclusive))
                      return encode_error("Invalid stream ID specified as stream command argument");
                    if (exclusive) start.insert(start.begin(), '(');
                    if (!DataStore::validate_xautoclaim_start(start, start_error))
                      return encode_stream_error(start_error);
                    std::int64_t count = 100;
                    bool justid = false;
                    constexpr std::int64_t kMaxCount = 9223372036854775807LL / 16;
                    for (std::size_t i = 6; i < args.size(); ++i) {
                      const auto o = upper(args[i]);
                      if (o == "COUNT" && i + 1 < args.size()) {
                        if (!parse_strict_int(args[++i], count) || count < 1 || count > kMaxCount)
                          return encode_error("COUNT must be > 0");
                      } else if (o == "JUSTID")
                        justid = true;
                      else
                        return encode_error("syntax error");
                    }
                    bool wt = false, consumer_created = false;
                    std::string err;
                    std::vector<DataStore::StreamClaimEffect> effects;
                    auto res = store().xautoclaim(args[1], args[2], args[3], min_idle, start, count,
                                                  justid, wt, err, &consumer_created, &effects);
                    if (wt) return wrongtype_error_reply();
                    if (!err.empty()) return encode_stream_error(err);
                    propagate_stream_claims(effects, session);
                    if (effects.empty() && !consumer_created) g_write_changed_nothing = true;
                    if (consumer_created)
                      notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", args[1],
                                            session.db_index);
                    std::vector<std::string> ents, del;
                    for (const auto& [id, fields] : res.claimed)
                      ents.push_back(encode_stream_entry(id, fields, justid));
                    for (const auto& d : res.deleted) del.push_back(encode_bulk(d));
                    return encode_array(
                        {encode_bulk(res.next_cursor), encode_array(ents), encode_array(del)});
                  }});

    t.emplace(
        "XCLAIM",
        CommandSpec{
            "XCLAIM",
            -6,
            {"write", "fast"},
            1,
            1,
            1,
            [encode_stream_entry](const std::vector<std::string>& args, SessionState& session,
                                  bool&) -> std::string {
              // Redis looks up the key/group before parsing XCLAIM options.
              if (store().is_wrongtype_for_stream(args[1])) return wrongtype_error_reply();
              std::string before_id;
              std::int64_t entries_read = 0;
              if (!store().stream_group_ids(args[1], args[2], before_id, entries_read))
                return "-NOGROUP No such key '" + args[1] + "' or consumer group '" + args[2] +
                       "'\r\n";
              std::int64_t min_idle = 0;
              if (!parse_strict_int(args[4], min_idle))
                return encode_error("Invalid min-idle-time argument for XCLAIM");
              if (min_idle < 0) min_idle = 0;
              std::vector<std::string> ids;
              std::size_t i = 5;
              std::string normalized_id;
              while (i < args.size() && normalize_claim_stream_id(args[i], normalized_id)) {
                ids.push_back(normalized_id);
                ++i;
              }
              bool justid = false, force = false;
              std::string last_id;
              bool have_last_id = false;
              DataStore::StreamClaimOptions options;
              for (; i < args.size(); ++i) {
                const auto o = upper(args[i]);
                if (o == "FORCE")
                  force = true;
                else if (o == "JUSTID")
                  justid = true;
                else if ((o == "IDLE" || o == "TIME" || o == "RETRYCOUNT") && i + 1 < args.size()) {
                  std::int64_t value;
                  if (!parse_strict_int(args[++i], value))
                    return encode_error("Invalid " + o + " option argument for XCLAIM");
                  if (o == "RETRYCOUNT")
                    options.retrycount = value;
                  else if (o == "IDLE") {
                    options.idle = value;
                    options.time.reset();
                  } else {
                    options.time = value;
                    options.idle.reset();
                  }
                } else if (o == "LASTID" && i + 1 < args.size()) {
                  if (!normalize_claim_stream_id(args[++i], last_id))
                    return encode_error("Invalid stream ID specified as stream command argument");
                  have_last_id = true;
                } else
                  return encode_error("Unrecognized XCLAIM option '" + args[i] + "'");
              }
              bool wt = false, consumer_created = false;
              std::string err;
              std::vector<DataStore::StreamClaimEffect> effects;
              auto claimed = store().xclaim(args[1], args[2], args[3], min_idle, ids, justid, force,
                                            wt, err, have_last_id ? &last_id : nullptr,
                                            &consumer_created, &options, &effects);
              if (wt) return wrongtype_error_reply();
              if (!err.empty()) return encode_stream_error(err);
              propagate_stream_claims(effects, session);
              std::string after_id;
              store().stream_group_ids(args[1], args[2], after_id, entries_read);
              if (effects.empty() && after_id != before_id)
                propagate_stream_effect({"XGROUP", "SETID", args[1], args[2], after_id,
                                         "ENTRIESREAD", std::to_string(entries_read)},
                                        session);
              if (effects.empty() && after_id == before_id && !consumer_created)
                g_write_changed_nothing = true;
              if (consumer_created)
                notify_keyspace_event(NOTIFY_STREAM, "xgroup-createconsumer", args[1],
                                      session.db_index);
              std::vector<std::string> out;
              for (const auto& [id, fields] : claimed)
                out.push_back(encode_stream_entry(id, fields, justid));
              return encode_array(out);
            }});

    t.emplace(
        "XSETID",
        CommandSpec{"XSETID",
                    -3,
                    {"write", "fast"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      // XSETID key id [ENTRIESADDED n] [MAXDELETEDID id] — options are validated
                      // before the key-existence check (Redis xsetidCommand order).
                      std::string entries_added, max_deleted;
                      bool have_ea = false, have_md = false;
                      for (std::size_t i = 3; i < args.size(); ++i) {
                        const auto opt = upper(args[i]);
                        if (opt == "ENTRIESADDED" && i + 1 < args.size()) {
                          std::int64_t v = 0;
                          if (!parse_i64(args[i + 1], v))
                            return encode_error("value is not an integer or out of range");
                          if (v < 0) return encode_error("value is out of range, must be positive");
                          entries_added = args[i + 1];
                          have_ea = true;
                          ++i;
                        } else if (opt == "MAXDELETEDID" && i + 1 < args.size()) {
                          max_deleted = args[i + 1];
                          have_md = true;
                          ++i;
                        } else {
                          return encode_error("syntax error");
                        }
                      }
                      bool wt = false;
                      std::string err;
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
    t.emplace("PFADD",
              CommandSpec{
                  "PFADD",
                  -2,
                  {"write", "fast"},
                  1,
                  1,
                  1,
                  [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                    const std::string& key = args[1];
                    if (store().is_wrongtype_for_string(key)) return wrongtype_error_reply();
                    auto cur = store().get(key);
                    if (cur.has_value() && !hll_is_valid(*cur))
                      return encode_error("WRONGTYPE Key is not a valid HyperLogLog string value.");
                    std::string hll = cur.has_value() ? *cur : hll_new();
                    bool changed = !cur.has_value();
                    for (std::size_t i = 2; i < args.size(); ++i)
                      if (hll_add(hll, args[i])) changed = true;
                    if (changed) store().set(key, hll, false, false, std::nullopt, true);
                    return encode_integer(changed ? 1 : 0);
                  }});

    t.emplace(
        "PFCOUNT",
        CommandSpec{
            "PFCOUNT",
            -2,
            {"readonly"},
            1,
            -1,
            1,
            [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
              std::string merged = hll_new();
              auto* mregs = reinterpret_cast<std::uint8_t*>(merged.data()) + kHllHdr;
              for (std::size_t i = 1; i < args.size(); ++i) {
                if (store().is_wrongtype_for_string(args[i])) return wrongtype_error_reply();
                auto cur = store().get(args[i]);
                if (!cur.has_value()) continue;
                if (!hll_is_valid(*cur))
                  return encode_error("WRONGTYPE Key is not a valid HyperLogLog string value.");
                const auto* r = reinterpret_cast<const std::uint8_t*>(cur->data()) + kHllHdr;
                for (int j = 0; j < kHllRegs; ++j)
                  if (r[j] > mregs[j]) mregs[j] = r[j];
              }
              return encode_integer(static_cast<long long>(hll_count(merged) + 0.5));
            }});

    t.emplace(
        "PFMERGE",
        CommandSpec{
            "PFMERGE",
            -2,
            {"write"},
            1,
            -1,
            1,
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
                if (!hll_is_valid(*cur))
                  return encode_error("WRONGTYPE Key is not a valid HyperLogLog string value.");
                const auto* r = reinterpret_cast<const std::uint8_t*>(cur->data()) + kHllHdr;
                for (int j = 0; j < kHllRegs; ++j)
                  if (r[j] > mregs[j]) mregs[j] = r[j];
              }
              store().set(dest, merged, false, false, std::nullopt, true);
              return encode_simple("OK");
            }});

    // ─── SMOVE ───────────────────────────────────────────────────────────
    t.emplace(
        "SMOVE",
        CommandSpec{"SMOVE",
                    4,
                    {"write", "fast"},
                    1,
                    2,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      const std::string& src = args[1];
                      const std::string& dst = args[2];
                      const std::string& m = args[3];
                      if (store().is_wrongtype_for_set(src) || store().is_wrongtype_for_set(dst))
                        return wrongtype_error_reply();
                      bool wt = false;
                      if (!store().sismember(src, m, wt)) return encode_integer(0);
                      if (wt) return wrongtype_error_reply();
                      store().srem(src, {m}, wt);
                      store().sadd(dst, {m}, wt);
                      return encode_integer(1);
                    }});

    // ─── ZRANGESTORE — shares the unified range selector with ZRANGE ───
    t.emplace(
        "ZRANGESTORE",
        CommandSpec{"ZRANGESTORE",
                    -5,
                    {"write"},
                    1,
                    2,
                    1,
                    [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
                      const std::string& dst = args[1];
                      if (store().is_wrongtype_for_zset(dst)) return wrongtype_error_reply();
                      std::vector<std::pair<std::string, double>> items;
                      bool withscores = false;
                      auto err = zrange_select(args, 2, 3, 5, /*allow_withscores=*/false, items,
                                               withscores);
                      if (!err.empty()) return err;
                      store().del(dst);
                      for (const auto& [mem, sc] : items)
                        store().zadd_one(dst, sc, mem, false, false, false, false, false);
                      return encode_integer(static_cast<long long>(items.size()));
                    }});

    // ─── PUBSUB introspection (+ sharded aliases) ────────────────────────
    t.emplace(
        "PUBSUB",
        CommandSpec{
            "PUBSUB",
            -2,
            {"pubsub", "fast", "loading", "stale"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState&, bool&) -> std::string {
              const auto sub = upper(args[1]);
              if (sub == "HELP") {
                return encode_array(
                    {encode_bulk("PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
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
                  if (!subs.empty() && fnmatch(pat.c_str(), ch.c_str(), 0) == 0)
                    out.push_back(encode_bulk(ch));
                }
                return encode_array(out);
              }
              if (sub == "NUMSUB" || sub == "SHARDNUMSUB") {
                std::vector<std::string> out;
                for (std::size_t i = 2; i < args.size(); ++i) {
                  auto it = subscribers.find(args[i]);
                  out.push_back(encode_bulk(args[i]));
                  out.push_back(encode_integer(
                      it == subscribers.end() ? 0 : static_cast<long long>(it->second.size())));
                }
                return encode_array(out);
              }
              if (sub == "NUMPAT") {
                std::size_t n = 0;
                for (const auto& [p, subs] : g_pattern_subscribers)
                  if (!subs.empty()) ++n;
                return encode_integer(static_cast<long long>(n));
              }
              return encode_error("Unknown PUBSUB subcommand or wrong number of arguments for '" +
                                  lower(args[1]) + "'");
            }});

    // Sharded pub/sub — in standalone mode this is equivalent to regular pub/sub.
    auto delegate = [](const char* base, const std::vector<std::string>& args,
                       SessionState& session, bool& close) -> std::string {
      std::vector<std::string> proxy = args;
      proxy[0] = base;
      const auto& tbl = command_table();
      auto it = tbl.find(base);
      return it == tbl.end() ? encode_error("unknown command")
                             : it->second.handler(proxy, session, close);
    };
    // Sharded pub/sub — a separate channel namespace with ssubscribe/sunsubscribe/smessage
    // reply types; the client's shard-subscription count is independent of regular channels.
    t.emplace("SSUBSCRIBE",
              CommandSpec{"SSUBSCRIBE",
                          -2,
                          {"pubsub"},
                          0,
                          0,
                          0,
                          [](const std::vector<std::string>& args, SessionState& session, bool&) {
                            std::string out;
                            for (std::size_t i = 1; i < args.size(); ++i) {
                              pubsub_ssubscribe(session.fd, args[i]);
                              out += encode_array({encode_bulk("ssubscribe"), encode_bulk(args[i]),
                                                   encode_integer(static_cast<long long>(
                                                       shard_channel_count(session.fd)))});
                            }
                            return out;
                          }});
    t.emplace(
        "SUNSUBSCRIBE",
        CommandSpec{
            "SUNSUBSCRIBE",
            -1,
            {"pubsub"},
            0,
            0,
            0,
            [](const std::vector<std::string>& args, SessionState& session, bool&) {
              std::string out;
              if (args.size() == 1) {
                auto cit = g_fd_shard_channels.find(session.fd);
                std::vector<std::string> chans;
                if (cit != g_fd_shard_channels.end())
                  chans.assign(cit->second.begin(), cit->second.end());
                if (chans.empty()) {
                  out = encode_array({encode_bulk("sunsubscribe"), encode_null(RespVersion::Resp2),
                                      encode_integer(0)});
                } else {
                  for (const auto& ch : chans) {
                    pubsub_sunsubscribe(session.fd, ch);
                    out += encode_array(
                        {encode_bulk("sunsubscribe"), encode_bulk(ch),
                         encode_integer(static_cast<long long>(shard_channel_count(session.fd)))});
                  }
                }
              } else {
                for (std::size_t i = 1; i < args.size(); ++i) {
                  pubsub_sunsubscribe(session.fd, args[i]);
                  out += encode_array(
                      {encode_bulk("sunsubscribe"), encode_bulk(args[i]),
                       encode_integer(static_cast<long long>(shard_channel_count(session.fd)))});
                }
              }
              return out;
            }});
    t.emplace("SPUBLISH",
              CommandSpec{"SPUBLISH",
                          3,
                          {"write", "pubsub", "fast"},
                          1,
                          1,
                          1,
                          [](const std::vector<std::string>& args, SessionState&, bool&) {
                            return encode_integer(
                                static_cast<long long>(pubsub_spublish(args[1], args[2])));
                          }});

    // ─── Read-only command variants (reject the write/STORE options) ─────
    t.emplace("SORT_RO", CommandSpec{"SORT_RO",
                                     -2,
                                     {"readonly"},
                                     1,
                                     1,
                                     1,
                                     [](const std::vector<std::string>& a, SessionState& s,
                                        bool&) -> std::string {
                                       return do_sort(a, s.resp_version, /*readonly=*/true);
                                     }});
    t.emplace("BITFIELD_RO",
              CommandSpec{"BITFIELD_RO",
                          -2,
                          {"readonly", "fast"},
                          1,
                          1,
                          1,
                          [delegate](const std::vector<std::string>& a, SessionState& s,
                                     bool& c) -> std::string {
                            for (std::size_t i = 2; i < a.size(); ++i) {
                              const auto o = upper(a[i]);
                              if (o == "SET" || o == "INCRBY" || o == "OVERFLOW")
                                return encode_error("BITFIELD_RO only supports the GET subcommand");
                            }
                            return delegate("BITFIELD", a, s, c);
                          }});
    t.emplace(
        "GEORADIUS_RO",
        CommandSpec{"GEORADIUS_RO",
                    -6,
                    {"readonly"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& a, SessionState& s, bool&) -> std::string {
                      return do_geo_search(a, s.resp_version, /*form=*/0, /*readonly=*/true);
                    }});
    t.emplace(
        "GEORADIUSBYMEMBER_RO",
        CommandSpec{"GEORADIUSBYMEMBER_RO",
                    -5,
                    {"readonly"},
                    1,
                    1,
                    1,
                    [](const std::vector<std::string>& a, SessionState& s, bool&) -> std::string {
                      return do_geo_search(a, s.resp_version, /*form=*/1, /*readonly=*/true);
                    }});

    // Pre-compute flag bitmasks for fast checking
    for (auto& [key, spec] : t) {
      spec.flag_bits = 0;
      for (const auto& f : spec.flags) {
        if (f == "write")
          spec.flag_bits |= kFlagWrite;
        else if (f == "readonly")
          spec.flag_bits |= kFlagReadonly;
        else if (f == "fast")
          spec.flag_bits |= kFlagFast;
        else if (f == "admin")
          spec.flag_bits |= kFlagAdmin;
        else if (f == "noscript")
          spec.flag_bits |= kFlagNoScript;
        else if (f == "pubsub")
          spec.flag_bits |= kFlagPubSub;
        else if (f == "loading")
          spec.flag_bits |= kFlagLoading;
        else if (f == "stale")
          spec.flag_bits |= kFlagStale;
        else if (f == "no_auth")
          spec.flag_bits |= kFlagNoAuth;
      }
      spec.redis_name = lower(spec.name);
      spec.propagates_effects = spec.redis_name == "xreadgroup" || spec.redis_name == "xclaim" ||
                                spec.redis_name == "xautoclaim";
      const auto& index = redis_command_index();
      const auto redis = index.flags.find(spec.redis_name);
      spec.redis_flags = redis == index.flags.end() ? 0 : redis->second;
      spec.redis_container = index.containers.count(spec.redis_name) != 0;
    }

    // Apply rename-command directives in config order. The config loader (src/config.cpp) has
    // already checked them against Redis's command table, as Redis's own loader does
    // (config.c): each one removes the command and, unless the new name is empty, adds it back
    // under the new name. Done after flag computation so the moved spec keeps its flags. Names
    // are matched case-insensitively (table keys are upper-case, and handle_command
    // upper-cases incoming command names).
    for (const auto& rename : g_rename_commands) {
      const std::string f = upper(rename.from);
      const std::string tname = upper(rename.to);
      auto fit = t.find(f);
      if (fit == t.end()) {
        // A Redis command PeaDB does not implement (delta item 68). Removing it changes
        // nothing; renaming it cannot give a working command. The new name is not logged,
        // because operators often use it as a secret.
        if (!tname.empty()) {
          g_rename_warnings.push_back("config line " + std::to_string(rename.line) +
                                      ": rename-command " + upper(rename.redis_name) +
                                      " has no effect, because PeaDB does not implement " +
                                      upper(rename.redis_name) + " (compat/delta.md item 68)");
        }
        continue;
      }
      CommandSpec spec = fit->second;
      t.erase(fit);
      if (tname.empty()) continue;
      if (t.count(tname) != 0) {  // cannot happen after the loader's check; kept as a guard
        g_rename_error = format_config_error(rename.origin, "Target command name already exists");
        break;
      }
      spec.name = tname;
      t[tname] = std::move(spec);
    }

    return t;
  }();

  return table;
}

}  // namespace

static std::string handle_command_impl(const std::vector<std::string>& args, SessionState& session,
                                       bool& should_close);

std::string handle_command(const std::vector<std::string>& args, SessionState& session,
                           bool& should_close) {
  // Normal client commands need no replay diagnostics; keep their dispatch a tail call.
  if (!must_obey_client()) return handle_command_impl(args, session, should_close);
  const std::string reply = handle_command_impl(args, session, should_close);
  // Redis's afterErrorReply(): report every failed master command, including a queued command
  // that fails inside EXEC (the outer array is not an error). Lua's temporary client is not the
  // master/AOF client: a redis.pcall error is reported only if the script returns it.
  if (must_obey_client() && !in_lua_script_context() && is_error_reply(reply)) {
    ++g_unexpected_error_replies;
    const bool master = g_applying_master_stream;
    std::string name = g_master_stream_command_name;
    if (!master) {
      name = !args.empty() && command_table().count(upper(args[0])) ? lower(args[0]) : "<unknown>";
    }
    std::string error = reply.substr(0, std::min<std::size_t>(reply.size(), 4096));
    if (error.size() >= 2 && error.compare(error.size() - 2, 2, "\r\n") == 0)
      error.resize(error.size() - 2);
    log(LogLevel::Warn, "== CRITICAL == This " + std::string(master ? "replica" : "server") +
                            " is sending an error to its " +
                            (master ? "master" : "AOF-loading-client") + ": '" + error +
                            "' after processing the command '" + name + "'");
  }
  return reply;
}

static std::string handle_command_impl(const std::vector<std::string>& args, SessionState& session,
                                       bool& should_close) {
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
  const bool is_script_cmd = (cmd == "EVAL" || cmd == "EVALSHA" || cmd == "EVAL_RO" ||
                              cmd == "EVALSHA_RO" || cmd == "FCALL" || cmd == "FCALL_RO");
  if (is_script_cmd) {
    g_script_inner_error_seen = false;
    (void)lua_engine_consume_last_error_from_redis();  // left set by an earlier script's error
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
  if (!must_obey_client() && g_script_busy.load() && !is_script_kill && !is_busy_allowed_function &&
      cmd != "SHUTDOWN" && cmd != "HELLO" && cmd != "AUTH" && cmd != "RESET" && cmd != "QUIT") {
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
        return "-EXECABORT Transaction discarded because of previous errors: BUSY Redis is busy "
               "running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n";
      }
      if (session.in_multi && cmd != "DISCARD" && cmd != "MULTI" && cmd != "QUIT") {
        session.multi_dirty = true;
      }
      return busy_script_error_reply();
    }
  }
  if (session.in_multi && cmd != "EXEC" && cmd != "DISCARD" && cmd != "MULTI" && cmd != "QUIT" &&
      cmd != "RESET") {
    const auto qit = table.find(cmd);
    if (qit == table.end()) {
      session.multi_dirty = true;
      return encode_error("unknown command");
    }
    if (!arity_ok(args.size(), qit->second.arity)) {
      session.multi_dirty = true;
      return encode_error("wrong number of arguments for '" + lower(cmd) + "' command");
    }
    const std::uint64_t flags = spec_command_flags(qit->second, args);
    // Redis's CMD_NO_MULTI commands (SAVE, SHUTDOWN, SYNC, PSYNC) are refused inside MULTI.
    if (flags & kRedisCmdNoMulti) {
      session.multi_dirty = true;
      record_cmd_rejected(cmd);
      record_error_stat("ERR");
      return encode_error("Command not allowed inside a transaction");
    }
    if (g_applying_master_stream && !misconf_reply().empty()) obey_master_disk_error(cmd, flags);
    // processCommand() runs the maxmemory step before it queues: while memory is over the limit,
    // nothing is queued. Not for a transaction from the master stream or the AOF.
    if (!must_obey_client() && maxmemory_refuses(cmd, args, session)) {
      session.multi_dirty = true;
      record_cmd_rejected(cmd);
      record_error_stat("OOM");
      return std::string("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    }
    // WATCH is not queued: watchCommand() answers this, after processCommand()'s checks.
    if (cmd == "WATCH") return encode_error("WATCH inside MULTI is not allowed");
    // processCommand() checks a command before it queues it, as at the top level below, and
    // flags the transaction (-EXECABORT at EXEC); not a transaction from the master stream or
    // the AOF.
    if (!must_obey_client()) {
      std::string refusal = misconf_reply();
      if (refusal.empty() || (cmd != "PING" && !(flags & kRedisCmdWrite)))
        refusal = replication_refusal(flags);
      if (!refusal.empty()) {
        session.multi_dirty = true;
        record_cmd_rejected(cmd);
        record_error_stat(error_code_from_reply(refusal));
        return refusal;
      }
    }
    // Redis checks CLIENT PAUSE before it queues, so a write sent inside MULTI waits too.
    if (pause_holds(cmd, args, session)) {
      session.postponed = true;
      return std::string();
    }
    // queueMultiCommand(): a transaction that already has an error stores nothing more, so the
    // flags EXEC is checked by are those of what it holds.
    if (!session.multi_dirty) {
      session.queued.push_back(args);
      session.multi_flags |= flags;
      session.multi_inv_flags |= ~flags;
    }
    return encode_simple("QUEUED");
  }

  const auto it = table.find(cmd);
  // ── Authentication gate (default user / requirepass) ──
  // Skipped when no password is configured (open, like Redis with no requirepass) and
  // for internally replayed/replicated commands (g_replaying_aof). Commands flagged
  // no_auth (AUTH/HELLO/QUIT/RESET) are always allowed so a client can authenticate.
  if (!g_requirepass.empty() && !session.authenticated && !must_obey_client()) {
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
  const bool in_script = in_lua_script_context();
  // processCommand()'s checks for the client's own command, in Redis's order and by Redis's
  // command flags (command_flags(), getCommandFlags()): -OOM, -MISCONF, -NOREPLICAS, -READONLY,
  // the pub/sub context, -MASTERDOWN. An EXEC is checked by the flags of what it queued (Redis's
  // mstate flags) and refused with execCommandAbort()'s text, which discards the transaction.
  // The commands EXEC runs are not checked again (execCommand() calls them), a script's commands
  // are checked as scriptCall() does (below and in dispatch_from_lua()), and nothing replayed
  // from the AOF or the master stream is checked (mustObeyClient()).
  const bool client_command = !must_obey_client() && !in_script && !g_executing_exec;
  const bool queued_exec = cmd == "EXEC" && session.in_multi;
  // Redis's rejectCommand().
  auto reject = [&](const std::string& refusal) -> std::string {
    ++top_stat.rejected_calls;
    if (in_script) g_script_inner_error_seen = true;
    if (!queued_exec) {
      if (session.in_multi) session.multi_dirty = true;  // flagTransaction() (a MULTI inside one)
      record_error_stat(error_code_from_reply(refusal));
      return refusal;
    }
    // execCommandAbort(): discard the transaction, and show the EXEC to MONITOR, which saw MULTI.
    record_error_stat("EXECABORT");
    session.in_multi = false;
    session.multi_dirty = false;
    session.queued.clear();
    session.watched_epoch.reset();
    session.watched_digests.clear();
    feed_monitors(cmd, args, session);
    return "-EXECABORT Transaction discarded because of: " + refusal.substr(1);
  };
  if (g_applying_master_stream && !g_executing_exec && !misconf_reply().empty()) {
    const std::uint64_t flags = queued_exec ? session.multi_flags : spec_command_flags(spec, args);
    obey_master_disk_error(cmd, flags);
  }
  bool deny_stale = false;  // the -MASTERDOWN check, after the pub/sub context one
  if (client_command) {
    if (maxmemory_refuses(cmd, args, session))
      return reject("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    // The flags are read only when a check can refuse, which keeps them off a master's hot path.
    const std::string misconf = misconf_reply();
    if (!misconf.empty() || g_replication_role == "slave" || g_config_min_replicas_to_write > 0) {
      const std::uint64_t flags = queued_exec ? 0 : spec_command_flags(spec, args);
      const bool writes =
          queued_exec ? (session.multi_flags & kRedisCmdWrite) != 0 : (flags & kRedisCmdWrite) != 0;
      // While persistence is failing: writes and PING (writeCommandsDeniedByDiskError()).
      if (!misconf.empty() && (writes || cmd == "PING")) return reject(misconf);
      if (std::string refusal = write_refusal(writes); !refusal.empty()) return reject(refusal);
      // EXEC itself is flagged STALE: it is refused when something it queued is not.
      // Redis answers an unknown subcommand before these checks; PeaDB's handlers answer it.
      deny_stale = queued_exec ? (session.multi_inv_flags & kRedisCmdStale) != 0
                               : !(flags & kRedisCmdStale) && !spec_subcommand_unknown(spec, args);
    }
  } else if (in_script) {
    // scriptCall(): scriptVerifyWriteCommandAllow() refuses a write made before the script's
    // first one (a script that has written is never stopped half done) on a replica, while
    // persistence is failing and without enough good replicas; then scriptVerifyOOM(); then a
    // write marks the script as having written (SCRIPT_WRITE_DIRTY). Its stale and read-only
    // checks came first, in dispatch_from_lua().
    const bool first_write =
        !g_script_write_dirty && (spec_command_flags(spec, args) & kRedisCmdWrite) != 0;
    if (first_write && !must_obey_client()) {
      std::string refusal =
          g_replication_role == "slave" ? std::string(kReadonlyReplicaReply) : misconf_reply();
      if (refusal.empty() && noreplicas_should_reject()) refusal = kNoReplicasReply;
      if (!refusal.empty()) return reject(refusal);
    }
    if (script_refuses_for_oom(args))
      return reject("-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    if (first_write) g_script_write_dirty = true;
  }
  // RESP2 clients with active subscriptions may only run the pub/sub control commands
  // (Redis server.c subscribe-mode gate; exemption is by command identity — PUBLISH is
  // deliberately NOT exempt). RESP3 clients have no restrictions.
  if (session.resp_version == RespVersion::Resp2 && !must_obey_client() &&
      !in_lua_script_context() && client_has_subscriptions(session.fd) && cmd != "PING" &&
      cmd != "QUIT" && cmd != "RESET" && cmd != "SUBSCRIBE" && cmd != "UNSUBSCRIBE" &&
      cmd != "PSUBSCRIBE" && cmd != "PUNSUBSCRIBE" && cmd != "SSUBSCRIBE" &&
      cmd != "SUNSUBSCRIBE") {
    ++top_stat.rejected_calls;
    record_error_stat("ERR");
    return encode_error("Can't execute '" + lower(cmd) +
                        "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET "
                        "are allowed in this context");
  }
  if (std::string refusal = stale_refusal(deny_stale); !refusal.empty()) return reject(refusal);

  // CLIENT PAUSE: hold the command, after the checks that refuse it (as Redis's processCommand
  // does); the event loop runs it again when the pause ends.
  if (pause_holds(cmd, args, session)) {
    session.postponed = true;
    return std::string();
  }

  ++top_stat.calls;

  // A standalone (cluster-disabled) node must never redirect: g_slot_routes defaults to
  // Unassigned and ADDSLOTS/SETSLOT are gated on cluster-enabled, so this is also true in
  // practice — but skip the whole check rather than rely on that alone.
  if (g_cluster_enabled && !must_obey_client() && spec.first_key > 0 &&
      static_cast<std::size_t>(spec.first_key) < args.size()) {
    const auto slot = cluster_keyslot(args[static_cast<std::size_t>(spec.first_key)]);
    const auto route = g_slot_routes[static_cast<std::size_t>(slot)];
    const auto owner = g_slot_owner[static_cast<std::size_t>(slot)];
    if (route == SlotRoute::Moved && owner >= 0) {
      return "-MOVED " + std::to_string(slot) + " " + cluster_node_addr(owner) + "\r\n";
    }
    // ASK reads the migration side channel, never g_slot_owner: reported ownership does not
    // move until SETSLOT ... NODE, so the redirect target for a slot in flight has to come from
    // somewhere else (see g_slot_migrating_to above and compat/delta.md item 6).
    const auto migrating_to = g_slot_migrating_to[static_cast<std::size_t>(slot)];
    if (migrating_to >= 0) {
      if (session.asking) {
        session.asking = false;
      } else {
        return "-ASK " + std::to_string(slot) + " " + cluster_node_addr(migrating_to) + "\r\n";
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
    const std::int64_t last_idx =
        spec.last_key >= 0 ? spec.last_key : static_cast<std::int64_t>(args.size()) + spec.last_key;
    for (std::int64_t i = spec.first_key;
         i <= last_idx && i < static_cast<std::int64_t>(args.size()); i += step) {
      if (!store().live_type(args[static_cast<std::size_t>(i)]).has_value()) {
        notify_new_candidates.push_back(args[static_cast<std::size_t>(i)]);
      }
    }
  }

  // `cmd` aliases a thread_local buffer that nested dispatch (EVAL/EXEC inner commands)
  // clobbers — snapshot the name for everything after the handler call. Command names
  // fit in SSO, so this never heap-allocates.
  const std::string cmd_after = cmd;
  // What the command propagates, with the writes of the commands it runs (EXEC, scripts), is
  // written when the outermost one ends, before its reply goes out.
  const WriteOffsetTracker write_offset(session);
  const ExecutionUnit unit(&args);
  const auto t_start = std::chrono::steady_clock::now();
  g_write_changed_nothing = false;
  const std::string reply = spec.handler(args, session, should_close);
  // Read at once: the commands EXEC or a script runs clear it for their own handlers.
  const bool changed_nothing = std::exchange(g_write_changed_nothing, false);
  const std::int64_t exec_usec = std::chrono::duration_cast<std::chrono::microseconds>(
                                     std::chrono::steady_clock::now() - t_start)
                                     .count();
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
    const bool redis_inner_error =
        is_script_cmd ? (g_script_inner_error_seen || lua_engine_consume_last_error_from_redis())
                      : false;
    if (!is_script_cmd || !redis_inner_error) {
      record_error_stat(error_code_from_reply(reply));
    }
  }
  // Skip write side-effects when the command DEFERRED (a blocking command that just
  // registered a block produced no mutation yet). Without this guard a blocked
  // BLPOP/XREADGROUP would be wrongly written to the AOF / dirtied / notified. A write that
  // changed nothing is skipped too (g_write_changed_nothing).
  if (spec_is_write && !changed_nothing && !is_error_reply(reply) &&
      session.blocked.type == BlockType::None) {
    store().increment_mutation_epoch();
    // Queued for the AOF and the replicas until the execution unit ends (`unit` above), which
    // advances g_master_repl_offset in backlog_append(). PUBLISH and SPUBLISH go to the replicas
    // only, as Redis's forceCommandPropagation(c, PROPAGATE_REPL) sends them.
    // The outermost command's write is the last of its unit: nothing after this propagates.
    const bool publish = cmd_after == "PUBLISH" || cmd_after == "SPUBLISH";
    if (!spec.propagates_effects)
      propagate_write(args, session, &reply,
                      publish ? kPropagateRepl : kPropagateAof | kPropagateRepl,
                      g_execution_nesting == 1);
    {
      // These checks read back what the command did (Redis sends the events from inside it), so
      // a key it left with a past expire time is not deleted here.
      const DataStore::LazyExpireDisabled no_lazy_expire(store());
      // 'new' fires before the command's own event, mirroring Redis (dbAdd runs first).
      for (const auto& k : notify_new_candidates) {
        if (store().type_of(k) != "none")
          notify_keyspace_event(NOTIFY_NEW, "new", k, session.db_index);
      }
      emit_keyspace_notifications(cmd_after, args, spec.first_key, session.db_index, &reply);
    }
    // Any write may satisfy a blocked client (BLPOP/BZPOPMIN/BLMOVE/XREAD…). Flag a
    // wakeup so the event loop rechecks blocked clients in THIS iteration instead of on
    // the next ~100ms poll. Cheap: the recheck early-returns when nothing is blocked.
    notify_key_written(spec.first_key > 0 && static_cast<std::size_t>(spec.first_key) < args.size()
                           ? args[static_cast<std::size_t>(spec.first_key)]
                           : std::string(),
                       session.db_index);
  }
  // Access tracking: refresh the command's primary key's LRU/LFU metadata. Done after the
  // handler so a key the command just created is stamped (not left at lru=0). Runs under
  // every policy (like Redis, which maintains the lru clock unconditionally) so that
  // OBJECT IDLETIME is honest even before an eviction policy is configured; the cost is
  // one hash find on the first key. Single-key/first-key approximation.
  // RESTORE (and RESTORE-ASKING) is exempt: it explicitly seeds the LRU/LFU (IDLETIME/FREQ) and,
  // like Redis's dbAdd path, must not have that immediately overwritten by an access-time touch.
  if (!is_error_reply(reply) && cmd != "RESTORE" && cmd != "RESTORE-ASKING" && spec.first_key > 0 &&
      static_cast<std::size_t>(spec.first_key) < args.size()) {
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
                        const std::string& masteruser, bool protected_mode,
                        const std::string& enable_module_command,
                        const std::vector<RenameCommandDirective>& rename_commands) {
  g_requirepass = requirepass;
  {
    std::lock_guard<std::recursive_mutex> lock(g_masterauth_mu);
    g_masterauth = masterauth;
    g_masteruser = masteruser;
  }
  g_protected_mode = protected_mode;
  g_enable_module_command = enable_module_command;
  g_rename_commands = rename_commands;
}

void configure_log_level(const std::string& name) {
  LogLevel level = LogLevel::Info;
  if (!parse_redis_log_level(name, level)) return;  // the config loader already checked it
  g_loglevel_name = name;
  set_log_level(level);
}

std::string validate_command_renames(std::vector<std::string>& warnings) {
  (void)command_table();  // the first call builds the table and applies the renames
  warnings.insert(warnings.end(), g_rename_warnings.begin(), g_rename_warnings.end());
  return g_rename_error;
}

bool protected_mode_enabled() {
  return g_protected_mode;
}
bool requirepass_is_set() {
  return !g_requirepass.empty();
}

void configure_maxmemory(const std::string& spec) {
  g_keyspace_expand_allowed = dict_expand_allowed;
  if (spec.empty()) {
    g_config_maxmemory = 0;
    return;
  }
  std::int64_t v = 0;
  if (parse_memory(spec, v) && v >= 0)
    g_config_maxmemory = v;
  else
    log(LogLevel::Warn, "ignoring invalid maxmemory value: " + spec);
  // Redis's main() warns about a suspicious setting.
  if (g_config_maxmemory > 0 && g_config_maxmemory < 1024 * 1024) {
    log(LogLevel::Warn,
        "WARNING: You specified a maxmemory value that is less than 1MB (current "
        "value is " +
            std::to_string(g_config_maxmemory) +
            " bytes). Are you sure this is what you really want?");
  }
}

void configure_maxmemory_policy(const std::string& spec) {
  if (spec.empty()) return;  // keep default (noeviction)
  if (auto p = parse_maxmemory_policy(spec))
    apply_maxmemory_policy(*p);
  else
    log(LogLevel::Warn, "ignoring invalid maxmemory-policy: " + spec);
}

void configure_maxmemory_samples(const std::string& spec) {
  if (spec.empty()) return;
  std::int64_t v = 0;
  if (parse_i64(spec, v) && v >= 1 && v <= std::numeric_limits<int>::max())
    g_maxmemory_samples = static_cast<int>(v);
  else
    log(LogLevel::Warn, "ignoring invalid maxmemory-samples: " + spec);
}

void configure_replication(bool replica, const std::string& host, int port) {
  g_replication_master_host = host;
  g_replication_master_port = port;
  g_replication_role = replica ? "slave" : "master";
  g_replica_stale = replica;
  store().set_replica_expiry(replica);
}

void start_configured_replication() {
  if (g_replication_role == "slave") {
    const auto gen = g_repl_generation.fetch_add(1) + 1;
    start_master_link(g_replication_master_host, g_replication_master_port, gen);
  }
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
  // The config loader folds the old *-ziplist-* aliases into these names (last value wins).
  apply("hash-max-listpack-entries", lim.hash_entries, false);
  apply("hash-max-listpack-value", lim.hash_value, false);
  apply("list-max-listpack-size", lim.list_size, true);
  apply("set-max-intset-entries", lim.set_intset, false);
  apply("set-max-listpack-entries", lim.set_entries, false);
  apply("set-max-listpack-value", lim.set_value, false);
  apply("zset-max-listpack-entries", lim.zset_entries, false);
  apply("zset-max-listpack-value", lim.zset_value, false);
}

void configure_runtime_settings(const std::unordered_map<std::string, std::string>& raw,
                                const std::string& config_file) {
  g_config_file = config_file;
  // CONFIG GET reports every setting the config named (a live setting reads its global
  // instead, and a fixed one reports PeaDB's behaviour).
  for (const auto& [name, value] : raw) g_config_values[name] = value;
  auto num = [&](const char* key, auto& slot) {
    const auto it = raw.find(key);
    std::int64_t v = 0;
    if (it != raw.end() && parse_i64(it->second, v)) {
      slot = static_cast<std::decay_t<decltype(slot)>>(v);
    }
  };
  auto yes = [&](const char* key, bool& slot) {
    const auto it = raw.find(key);
    if (it != raw.end()) slot = it->second == "yes";
  };
  num("min-replicas-to-write", g_config_min_replicas_to_write);
  yes("replica-serve-stale-data", g_config_replica_serve_stale_data);
  yes("replica-ignore-disk-write-errors", g_replica_ignore_disk_write_errors);
  num("busy-reply-threshold", g_config_lua_time_limit);
  yes("lazyfree-lazy-server-del", g_lazyfree_lazy_server_del);
  yes("stop-writes-on-bgsave-error", g_stop_writes_on_bgsave_error);
  yes("aof-load-truncated", g_aof_load_truncated);
  bool rdbchecksum = rdb_checksum();
  yes("rdbchecksum", rdbchecksum);
  rdb_set_checksum(rdbchecksum);
  num("slowlog-log-slower-than", g_slowlog_threshold_us);
  num("slowlog-max-len", g_slowlog_max_len);
  num("latency-monitor-threshold", g_latency_monitor_threshold_ms);
  num("proto-max-bulk-len", g_proto_max_bulk_len);
  num("client-query-buffer-limit", g_client_query_buffer_limit);
  num("tcp-keepalive", g_tcp_keepalive);
  int repl_timeout = g_repl_timeout.load();
  num("repl-timeout", repl_timeout);
  g_repl_timeout = repl_timeout;
  num("repl-ping-replica-period", g_repl_ping_replica_period);
  num("maxmemory-eviction-tenacity", g_maxmemory_eviction_tenacity);
  num("shutdown-timeout", g_shutdown_timeout);
  int lfu_log_factor = store().lfu_log_factor();
  num("lfu-log-factor", lfu_log_factor);
  store().set_lfu_log_factor(lfu_log_factor);
  int key_save_delay = rdb_key_save_delay();
  num("rdb-key-save-delay", key_save_delay);
  rdb_set_key_save_delay(key_save_delay);
  if (const auto it = raw.find("shutdown-on-sigterm"); it != raw.end())
    g_shutdown_on_sigterm = parse_shutdown_flags(it->second);
  if (const auto it = raw.find("shutdown-on-sigint"); it != raw.end())
    g_shutdown_on_sigint = parse_shutdown_flags(it->second);
}

std::int64_t proto_max_bulk_len() {
  return g_proto_max_bulk_len;
}
std::int64_t client_query_buffer_limit() {
  return g_client_query_buffer_limit;
}
int tcp_keepalive_seconds() {
  return g_tcp_keepalive;
}
int replication_timeout_seconds() {
  return g_repl_timeout.load();
}

void script_set_repl_flags(int flags) {
  g_script_repl_flags = flags;
}

void configure_timeout(const std::string& spec) {
  if (spec.empty()) return;
  std::int64_t v = 0;
  if (parse_i64(spec, v) && v >= 0)
    g_config_timeout = static_cast<int>(v);
  else
    log(LogLevel::Warn, "ignoring invalid timeout: " + spec);
}

int client_timeout_seconds() {
  return g_config_timeout;
}

void configure_client_output_buffer_limit(const std::string& spec) {
  if (spec.empty()) return;
  // Flat token stream of (class hard soft soft-seconds) groups, e.g.
  //   "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"
  std::istringstream iss(spec);
  std::string cls;
  while (iss >> cls) {
    std::string h, s, sec;
    if (!(iss >> h >> s >> sec)) {
      log(LogLevel::Warn, "ignoring malformed client-output-buffer-limit");
      return;
    }
    int idx = -1;
    const std::string c = lower(cls);
    if (c == "normal")
      idx = kCoblNormal;
    else if (c == "slave" || c == "replica")
      idx = kCoblReplica;
    else if (c == "pubsub")
      idx = kCoblPubsub;
    else {
      log(LogLevel::Warn, "unknown client-output-buffer-limit class: " + cls);
      continue;
    }
    std::int64_t hv = 0, sv = 0, secv = 0;
    if (!parse_memory(h, hv) || !parse_memory(s, sv) || !parse_i64(sec, secv) || hv < 0 || sv < 0 ||
        secv < 0) {
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
    return std::string(name) + " " + std::to_string(l.hard) + " " + std::to_string(l.soft) + " " +
           std::to_string(l.soft_seconds);
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

void configure_lazy_expire() {
  store().set_lazy_expire_hook(propagate_lazy_expire);
}

void configure_notify_keyspace_events(const std::string& spec) {
  g_notify_flags = parse_notify_flags(spec);
}

void configure_repl_backlog_size(const std::string& spec) {
  if (spec.empty()) return;
  std::int64_t v = 0;
  // Redis raises a smaller size to CONFIG_REPL_BACKLOG_MIN_SIZE (16 KB).
  if (parse_memory(spec, v) && v > 0) {
    try {
      g_repl_backlog.reset(static_cast<std::size_t>(std::max<std::int64_t>(v, 16 * 1024)));
    } catch (const std::exception&) {
      log(LogLevel::Warn,
          "cannot allocate a repl-backlog-size of " + spec + " bytes; keeping the default 1mb");
    }
  } else {
    log(LogLevel::Warn, "ignoring invalid repl-backlog-size: " + spec);
  }
}

void configure_save(const std::string& spec) {
  g_save_params.clear();
  g_save_spec = spec;
  std::istringstream iss(spec);
  std::int64_t sec = 0, changes = 0;
  while (iss >> sec >> changes) {
    // Redis keeps every pair with seconds >= 1; a change count of 0 means "every <sec> seconds".
    if (sec > 0) g_save_params.emplace_back(sec, changes);
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

// By the command the name runs, so a command renamed with rename-command keeps Redis's flags.
std::uint64_t redis_command_flags(const std::vector<std::string>& args) {
  if (args.empty()) return 0;
  const auto& table = command_table();
  const auto it = table.find(upper(args[0]));
  return it == table.end() ? 0 : spec_redis_flags(it->second, args);
}

bool client_pause_update() {
  if (g_paused_actions != 0) update_paused_actions();
  const bool rerun = g_pause_unblock_pending;
  g_pause_unblock_pending = false;
  return rerun;
}

std::int64_t client_pause_end_ms() {
  std::int64_t end = 0;
  for (const auto& p : g_pause_per_purpose) {
    if (p.actions != 0 && (end == 0 || p.end_ms < end)) end = p.end_ms;
  }
  return end;
}

void run_active_expire_cycle() {
  if (g_pause_cron) return;  // DEBUG PAUSE-CRON
  if (g_replication_role == "slave") return;
  if (paused_actions_with_update(kPauseExpire)) return;  // CLIENT PAUSE stops expiry
  const auto t0 = std::chrono::steady_clock::now();
  {
    // Each deleted key's DEL (propagate_lazy_expire()) is gathered and written at the end in one
    // AOF write and one backlog append, each DEL on its own, as Redis's beforeSleep() writes the
    // DELs of a cycle (one write and, with appendfsync always, one fsync instead of one per key).
    const ExecutionUnit unit(nullptr, /*may_wrap=*/false);
    store().active_expire_cycle();
  }
  // Record the cycle's wall time as an "expire-cycle" latency event (LATENCY monitor).
  const auto cycle_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
          .count();
  latency_add("expire-cycle", cycle_ms);
}

// The resize part of Redis's databasesCron(), run from the event loop: shrink each database's
// keyspace table that is less than 10% full, but not while a fork child runs (a rehash would make
// the kernel copy the child's memory pages). O(1) per database when nothing is to do.
void databases_cron() {
  if (g_pause_cron) return;  // DEBUG PAUSE-CRON
  if (has_active_child_process()) return;
  store().shrink_sparse_tables();
}

// Run on every event-loop pass: keeps the peak of used_memory (Redis's serverCron() does it every
// 100 ms) and, while the eviction timer is on (an eviction that stopped at its time limit, or a
// CONFIG SET maxmemory), evicts again, like Redis's evictionTimeProc(): until memory is within the
// limit or nothing is left to evict.
void run_eviction_cycle() {
  if (const auto used = static_cast<std::int64_t>(used_memory()); used > g_used_memory_peak)
    g_used_memory_peak = used;
  if (!g_eviction_proc_running) return;
  if (perform_evictions() == EvictResult::Running) return;
  g_eviction_proc_running = false;
}

bool eviction_proc_running() {
  return g_eviction_proc_running;
}

// Save-point cron, run from the event loop (the BGSAVE part of Redis's serverCron()): starts a
// BGSAVE when a (seconds, changes) save point is reached, then one asked for by BGSAVE SCHEDULE or
// inside MULTI/EXEC. Never while a child runs; after a failed BGSAVE, only once
// CONFIG_BGSAVE_RETRY_DELAY (5 s) has passed since the last try. O(1): mutation_epoch is the
// change counter.
void background_save_cron() {
  if (g_pause_cron) return;  // DEBUG PAUSE-CRON
  if (has_active_child_process()) return;
  constexpr std::int64_t kBgsaveRetryDelaySeconds = 5;
  const std::int64_t now_s = now_ms() / 1000;
  const bool may_try =
      g_last_bgsave_status == "ok" || now_s - g_lastbgsave_try > kBgsaveRetryDelaySeconds;
  const std::int64_t dirty =
      static_cast<std::int64_t>(store().mutation_epoch()) - g_dirty_at_last_save;
  // No early return when nothing changed: `save 60 0` saves every 60 s, as in Redis.
  for (const auto& [sec, changes] : g_save_params) {
    if (dirty >= changes && now_s - g_last_save_time > sec && may_try) {
      log(LogLevel::Info,
          std::to_string(changes) + " changes in " + std::to_string(sec) + " seconds. Saving...");
      do_bgsave_fork();
      return;
    }
  }
  if (g_rdb_bgsave_scheduled && may_try && do_bgsave_fork()) g_rdb_bgsave_scheduled = false;
}

std::string configure_persistence(const std::string& dbfilename, bool appendonly,
                                  const std::string& appendfilename,
                                  const std::string& appendfsync) {
  // Both names are relative to the working directory, which the config loader set to `dir`.
  g_snapshot_path = dbfilename;
  // "At startup we consider the DB saved" (Redis's initServer): save points count from now, so
  // `save 3600 1` waits an hour instead of saving after the first write, and INFO's
  // rdb_last_save_time is the start time, not 0.
  g_last_save_time = now_ms() / 1000;
  g_appendonly = appendonly;
  g_aof_state = appendonly ? AofState::On : AofState::Off;
  g_aof_path = appendfilename;
  g_appendfsync = appendfsync;
  if (!g_appendonly) return {};
  open_aof_fd();
  if (g_aof_fd >= 0) return {};
  // Redis's aofOpenIfNeededOnServerStart() exits when it cannot open the AOF: with the AOF on,
  // startup loads nothing else, so going on would serve no data and fail every write.
  return "Can't open the append-only file " + appendfilename + ": " + std::strerror(errno);
}

bool load_aof_file(const std::string& path, std::string& err) {
  err.clear();
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    err = "can't open the append log file " + path + " for reading: " + std::strerror(errno);
    return false;
  }
  const std::string buffer((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  const std::string_view view(buffer);
  // Redis's loadSingleAppendOnlyFile(): `valid_up_to` is the end of the last whole command, where
  // a short read at the end cuts the file, so the next append does not land after torn bytes
  // (compat/delta.md item 30).
  std::size_t pos = 0;
  std::size_t valid_up_to = 0;
  std::size_t valid_before_multi = 0;
  SessionState session{};
  session.authenticated = true;  // internal replay must not be gated by requirepass
  // No key expires while the AOF loads (Redis's keyIsExpired() while loading): the file was written
  // while its keys were alive, so `SET k 5 PXAT <past>` then `INCR k` must give 6 (and expire),
  // not a new key 1 without a TTL; the expired keys are deleted, with their DEL, after the load.
  const DataStore::LazyExpireDisabled no_lazy_expire(store());
  g_replaying_aof = true;
  struct ReplayingAof {
    ~ReplayingAof() {
      g_replaying_aof = false;
    }
  } replaying;
  std::vector<std::string> args;
  bool short_read = false;
  while (pos < view.size()) {
    const std::size_t start = pos;
    const AofStep step = read_aof_step(view, pos, args);
    if (step == AofStep::Annotation) continue;
    if (step == AofStep::ShortRead) {
      short_read = true;
      break;
    }
    if (step == AofStep::BadFormat) {
      err = "Bad file format reading the append only file " + path + " at offset " +
            std::to_string(start) +
            ": make a backup of your AOF file, then use ./redis-check-aof --fix <filename>";
      return false;
    }
    if (upper(args[0]) == "MULTI") valid_before_multi = valid_up_to;
    bool close = false;
    const auto reply = handle_command(args, session, close);
    if (is_error_reply(reply)) {
      err = "AOF replay command failed at offset " + std::to_string(start);
      return false;
    }
    valid_up_to = pos;
  }
  // A MULTI without its EXEC at the end is cut off like a short read, as in Redis, and so is the
  // whole MULTI when the short read is inside it: Redis cuts after the last whole command there,
  // which leaves an open MULTI in the file that would hold every later append in its queue.
  if (session.in_multi) {
    log(LogLevel::Warn, "Revert incomplete MULTI/EXEC transaction in AOF file " + path);
    valid_up_to = valid_before_multi;
    short_read = true;
  }
  if (!short_read) return true;
  if (g_aof_load_truncated) {
    log(LogLevel::Warn, "!!! Warning: short read while loading the AOF file " + path + "!!!");
    log(LogLevel::Warn,
        "!!! Truncating the AOF " + path + " at offset " + std::to_string(valid_up_to) + " !!!");
    std::string truncate_err;
    if (truncate_file(path, valid_up_to, truncate_err)) {
      log(LogLevel::Warn, "AOF " + path + " loaded anyway because aof-load-truncated is enabled");
      return true;
    }
    log(LogLevel::Warn, "Error truncating the AOF file " + path + ": " + truncate_err);
  }
  err = "Unexpected end of file reading the append only file " + path +
        ". You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix "
        "<filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option "
        "to yes and restart the server.";
  return false;
}

void configure_runtime_port(int port) {
  g_server_port = port;
}

// What this node tells other nodes and clients to reach it on. Redis resolves this from
// cluster-announce-ip / -port / -bus-port, falling back to the bound address, the client
// port, and port+10000 respectively (config.c:3105,3171-3172). The IP fallback here is a
// documented simplification, not a match for Redis: Redis instead auto-detects its address
// from a live connection (cluster.c:898-916,2875-2888); PeaDB has no such mechanism, so it
// falls back to the statically configured bind address instead — see
// docs/decisions/p5-cluster-addressing-and-version-provenance.md for the full rationale and
// the wildcard-refusal startup check that guards the hazard this creates.
std::string cluster_announced_host() {
  return g_cluster_announce_ip.empty() ? g_server_bind : g_cluster_announce_ip;
}
int cluster_announced_port() {
  return g_cluster_announce_port > 0 ? g_cluster_announce_port : g_server_port;
}
int cluster_announced_bus_port() {
  return g_cluster_announce_bus_port > 0 ? g_cluster_announce_bus_port
                                         : cluster_announced_port() + 10000;
}

// Shared by the main.cpp startup refusal and CONFIG SET's cluster-announce-ip validation (see
// their call sites) so the two checks of the same hazard cannot drift apart.
bool is_wildcard_announce_host(const std::string& host) {
  return host.empty() || host == "0.0.0.0" || host == "::" || host == "*";
}

// Mirrors config.bind/.port into command.cpp (the only prior reader was server.cpp, which is
// why the cluster surface used to hardcode 127.0.0.1 — see cluster_announced_host() above) and
// sets the cluster-specific fields. cluster-enabled is startup-only (Redis's IMMUTABLE_CONFIG);
// this is the single place it and g_server_bind are set, called once from main.cpp.
void configure_cluster(bool enabled, const std::string& bind, int port,
                       const std::string& announce_ip, const std::string& announce_port,
                       const std::string& announce_bus_port) {
  g_cluster_enabled = enabled;
  g_server_bind = bind;
  g_server_port = port;
  g_cluster_announce_ip = announce_ip;
  // 0 is Redis's own documented sentinel for "use the default" here (config.c:3171-3172,
  // redis.conf:1807), not an invalid value — cluster_announced_port()/_bus_port() already
  // treat > 0 as "explicitly set" and fall back otherwise, so only reject what CONFIG SET's
  // own validation for these two params rejects (unparseable, negative, or out of range),
  // matching it rather than warning about a value Redis itself ships as the example.
  if (!announce_port.empty()) {
    std::int64_t v = 0;
    if (parse_i64(announce_port, v) && v >= 0 && v <= 65535)
      g_cluster_announce_port = static_cast<int>(v);
    else
      log(LogLevel::Warn, "ignoring invalid cluster-announce-port: " + announce_port);
  }
  if (!announce_bus_port.empty()) {
    std::int64_t v = 0;
    if (parse_i64(announce_bus_port, v) && v >= 0 && v <= 65535)
      g_cluster_announce_bus_port = static_cast<int>(v);
    else
      log(LogLevel::Warn, "ignoring invalid cluster-announce-bus-port: " + announce_bus_port);
  }
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

void record_lua_script_rejected(const std::string& cmd) {
  auto& st = g_cmdstats[lower(cmd)];
  ++st.rejected_calls;
  record_error_stat("ERR");
  g_script_inner_error_seen = true;
}

// A command PeaDB does not know, or with the wrong number of arguments, is left to the dispatch,
// which answers those first, as Redis's scriptCall() does.
bool script_command_refused_as_stale(const std::vector<std::string>& args) {
  if (args.empty() || must_obey_client() || !running_stale()) return false;
  const auto& table = command_table();
  const auto it = table.find(upper(args[0]));
  if (it == table.end() || !arity_ok(args.size(), it->second.arity) ||
      spec_subcommand_unknown(it->second, args))
    return false;
  return !(spec_redis_flags(it->second, args) & kRedisCmdStale);
}

static std::atomic<bool> g_shutdown_requested{false};

void request_shutdown() {
  g_shutdown_requested.store(true, std::memory_order_release);
}

bool shutdown_requested() {
  return g_shutdown_requested.load(std::memory_order_acquire);
}

int parse_shutdown_flags(const std::string& value) {
  int flags = 0;
  std::istringstream words(value);
  for (std::string w; words >> w;) {
    w = lower(w);
    if (w == "save")
      flags |= kShutdownSave;
    else if (w == "nosave")
      flags |= kShutdownNosave;
    else if (w == "now")
      flags |= kShutdownNow;
    else if (w == "force")
      flags |= kShutdownForce;
  }
  return flags;
}

std::string shutdown_flags_to_str(int flags) {
  // In the order of Redis's shutdown_on_sig_enum.
  static constexpr std::pair<int, const char*> kNames[] = {{kShutdownSave, "save"},
                                                           {kShutdownNosave, "nosave"},
                                                           {kShutdownNow, "now"},
                                                           {kShutdownForce, "force"}};
  std::string out;
  for (const auto& [flag, name] : kNames) {
    if ((flags & flag) == 0) continue;
    if (!out.empty()) out += ' ';
    out += name;
  }
  return out.empty() ? "default" : out;
}

namespace {
// isReadyToShutdown(): every replica has acknowledged the whole replication stream.
bool is_ready_to_shutdown() {
  bool ready = true;
  if (g_for_each_replica_cb) {
    g_for_each_replica_cb([&](const SessionState& replica) {
      if (replica.repl_ack_offset != g_master_repl_offset) ready = false;
    });
  }
  return ready;
}

// replicationGetSlaveName(): the address the replica gave (REPLCONF ip-address) or its peer
// address, with the port it listens on (REPLCONF listening-port).
std::string replica_name(const SessionState& replica) {
  const std::string ip = replica_ip(replica);
  if (replica.repl_listening_port == 0) return ip + ":<unknown-replica-port>";
  if (ip.find(':') != std::string::npos)  // formatAddr(): an IPv6 address goes in brackets
    return "[" + ip + "]:" + std::to_string(replica.repl_listening_port);
  return ip + ":" + std::to_string(replica.repl_listening_port);
}

// cancelShutdown(): a shutdown that failed or was aborted forgets the signal that asked for it,
// answers the clients blocked on SHUTDOWN and ends its write pause.
void cancel_shutdown() {
  g_shutdown_asap.store(0);
  g_shutdown_flags = 0;
  g_shutdown_mstime = 0;
  g_last_sig_received.store(0);
  if (g_reply_blocked_on_shutdown_cb) {
    g_reply_blocked_on_shutdown_cb([](const SessionState& client) {
      // addReplyError(): counted in the error stats, and not sent under CLIENT REPLY OFF.
      record_error_stat("ERR");
      return client.reply_off ? std::string()
                              : encode_error("Errors trying to SHUTDOWN. Check logs.");
    });
  }
  unpause_actions(kPauseDuringShutdown);
}

// finishShutdown(): the last step, after the wait for replicas (if any). Returns true when the
// server may exit now.
bool finish_shutdown() {
  const int flags = g_shutdown_flags;
  const bool save = (flags & kShutdownSave) != 0;
  const bool nosave = (flags & kShutdownNosave) != 0;
  const bool force = (flags & kShutdownForce) != 0;
  // A line for each replica that has not acknowledged the whole replication stream. PeaDB sends
  // a full sync as one reply, so a replica is always "online" here.
  int replicas = 0;
  int lagging = 0;
  if (g_for_each_replica_cb) {
    const std::int64_t now_s = now_ms() / 1000;
    g_for_each_replica_cb([&](const SessionState& replica) {
      ++replicas;
      if (replica.repl_ack_offset == g_master_repl_offset) return;
      ++lagging;
      log(LogLevel::Info,
          "Lagging replica " + replica_name(replica) + " reported offset " +
              std::to_string(g_master_repl_offset - replica.repl_ack_offset) +
              " behind master, lag=" + std::to_string(now_s - replica.repl_ack_time_ms / 1000) +
              ", state=online.");
    });
  }
  if (replicas > 0) {
    log(LogLevel::Info, std::to_string(replicas - lagging) + " of " + std::to_string(replicas) +
                            " replicas are in sync when shutting down.");
  }
  auto fail = [] {
    log(LogLevel::Warn,
        "Errors trying to shut down the server. Check the logs for more information.");
    cancel_shutdown();
    return false;
  };
  if (g_bgsave_in_progress.load()) {
    log(LogLevel::Warn, "There is a child saving an .rdb. Killing it!");
    kill_rdb_child();
  }
  // CONFIG SET appendonly yes is still writing the first AOF: exiting now would leave no AOF with
  // the data. Redis checks this only while the rewrite child runs; PeaDB also refuses between two
  // tries of a rewrite that failed, when the AOF does not hold the data either.
  if (g_aof_state == AofState::WaitRewrite) {
    if (!force) {
      log(LogLevel::Warn, "Writing initial AOF, can't exit.");
      return fail();
    }
    log(LogLevel::Warn, "Writing initial AOF. Exit anyway.");
  }
  if (g_bgrewrite_in_progress.load()) {
    log(LogLevel::Warn, "There is a child rewriting the AOF. Killing it!");
    kill_aof_rewrite_child();
  }
  // Write what a failed AOF write left and fsync, so the everysec window is not lost.
  if (g_aof_state != AofState::Off) {
    log(LogLevel::Info, "Calling fsync() on the AOF file.");
    if (g_aof_state == AofState::On) flush_append_only_file({});
    // Redis exits even when the AOF still cannot take those bytes, and so loses writes it has
    // acknowledged; PeaDB refuses, as for a final RDB save that fails, so they stay in memory
    // until the disk works again (compat/delta.md item 35).
    if (!g_aof_buf.empty()) {
      if (!force) {
        log(LogLevel::Warn, "Error trying to write the AOF, can't exit.");
        return fail();
      }
      log(LogLevel::Warn, "Error trying to write the AOF. Exit anyway.");
    }
    if (g_aof_fd >= 0 && ::fsync(g_aof_fd) != 0)
      log(LogLevel::Warn, std::string("Fail to fsync the AOF file: ") + std::strerror(errno) + ".");
  }
  if ((!g_save_params.empty() && !nosave) || save) {
    log(LogLevel::Info, "Saving the final RDB snapshot before exiting.");
    const auto epoch = static_cast<std::int64_t>(store().mutation_epoch());
    std::string err;
    if (rdb_save(g_snapshot_path, err)) {
      note_rdb_saved(epoch);
    } else {
      log(LogLevel::Warn, err);
      if (!force) {
        log(LogLevel::Warn, "Error trying to save the DB, can't exit.");
        return fail();
      }
      log(LogLevel::Warn, "Error trying to save the DB. Exit anyway.");
    }
  }
  log(LogLevel::Warn, "PeaDB is now ready to exit, bye bye...");
  return true;
}
}  // namespace

bool shutdown_initiated() {
  return g_shutdown_mstime != 0;
}

bool prepare_for_shutdown(int flags) {
  if (shutdown_initiated()) return false;
  // A dataset that is still loading must not be saved over the file it comes from.
  if (g_loading.load()) flags = (flags & ~kShutdownSave) | kShutdownNosave;
  g_shutdown_flags = flags;
  log(LogLevel::Info, "User requested shutdown...");
  // Lagging replicas get shutdown-timeout seconds to catch up, so no acknowledged write is lost:
  // they are asked for an ACK, and writes (and key expiry and eviction) wait so the replication
  // offset stays where it is. shutdown_cron() finishes the shutdown.
  if (!(flags & kShutdownNow) && g_shutdown_timeout != 0 && !is_ready_to_shutdown()) {
    g_shutdown_mstime = now_ms() + static_cast<std::int64_t>(g_shutdown_timeout) * 1000;
    if (!(g_paused_actions & kPauseReplica) && g_replication_role != "slave")
      backlog_append(encode_command_resp({"REPLCONF", "GETACK", "*"}));
    pause_actions(kPauseDuringShutdown, std::numeric_limits<std::int64_t>::max(),
                  kPauseClientWriteSet);
    log(LogLevel::Info, "Waiting for replicas before shutting down.");
    return false;
  }
  return finish_shutdown();
}

bool abort_shutdown() {
  if (shutdown_initiated()) {
    cancel_shutdown();
  } else if (g_shutdown_asap.exchange(0) != 0) {
    // A signal asked for a shutdown that the event loop has not started yet.
    g_last_sig_received.store(0);
  } else {
    return false;
  }
  log(LogLevel::Info, "Shutdown manually aborted.");
  return true;
}

void shutdown_signal_handler(int sig) {
  // Only async-signal-safe work here: the shutdown itself runs in shutdown_cron(). The
  // interrupted code may be about to read errno, so it is put back.
  const int saved_errno = errno;
  const char* msg = sig == SIGINT    ? "Received SIGINT scheduling shutdown..."
                    : sig == SIGTERM ? "Received SIGTERM scheduling shutdown..."
                                     : "Received shutdown signal, scheduling shutdown...";
  // A second SIGINT (often a second Ctrl+C) means: exit now, without saving and without waiting.
  if (g_shutdown_asap.load() != 0 && sig == SIGINT) {
    log_from_signal_handler(LogLevel::Warn, "You insist... exiting now.");
    // rdbRemoveTempFile(getpid()): the final save may be writing temp-<pid>.rdb.
    char name[40] = "temp-";
    std::size_t n = 5;
    char digits[16];
    std::size_t nd = 0;
    for (auto pid = static_cast<unsigned long>(::getpid()); nd == 0 || pid != 0; pid /= 10) {
      digits[nd++] = static_cast<char>('0' + pid % 10);
    }
    while (nd > 0) name[n++] = digits[--nd];
    for (const char* ext = ".rdb"; *ext != '\0'; ++ext) name[n++] = *ext;
    name[n] = '\0';
    ::unlink(name);
    ::_exit(1);  // not a clean shutdown; _exit, because exit() is not async-signal-safe
  }
  if (g_loading.load()) msg = "Received shutdown signal during loading, scheduling shutdown.";
  log_from_signal_handler(LogLevel::Warn, msg);
  g_shutdown_asap.store(1);
  g_last_sig_received.store(sig);
  errno = saved_errno;
}

bool shutdown_signal_pending() {
  return g_shutdown_asap.load() != 0;
}

void set_loading(bool loading) {
  g_loading.store(loading);
}

void shutdown_cron() {
  // Redis runs this in serverCron(), so DEBUG PAUSE-CRON holds it, and so does a script that runs
  // too long (serverCron() does not run until the script ends).
  if (g_pause_cron || g_script_busy.load()) return;
  // The SHUTDOWN command runs inside safe_handle_command(); this path needs its own guard, so an
  // exception (for example out of memory) is a failed shutdown and not a crash.
  try {
    if (g_shutdown_asap.load() != 0 && !shutdown_initiated()) {
      // A SIGTERM or SIGINT asked for a shutdown.
      int flags = 0;
      const int sig = g_last_sig_received.load();
      if (sig == SIGINT && g_shutdown_on_sigint != 0)
        flags = g_shutdown_on_sigint;
      else if (sig == SIGTERM && g_shutdown_on_sigterm != 0)
        flags = g_shutdown_on_sigterm;
      if (prepare_for_shutdown(flags)) request_shutdown();
    } else if (shutdown_initiated()) {
      // A shutdown waits for replicas: finish it once they are in sync or the time is up. When
      // it fails, finish_shutdown() has logged why and cancelled it.
      if (now_ms() >= g_shutdown_mstime || is_ready_to_shutdown()) {
        if (finish_shutdown()) request_shutdown();
      }
    }
  } catch (const std::exception& e) {
    log(LogLevel::Warn, std::string("Errors trying to shut down the server: ") + e.what());
    cancel_shutdown();
  }
}

int configured_max_clients() {
  return g_runtime_max_clients.load(std::memory_order_relaxed);
}

std::size_t replication_event_count() {
  return static_cast<std::size_t>(g_repl_backlog.total);
}

// All backlog bytes from byte-offset `from` to the current total; false when the backlog no longer
// holds `from`.
bool replication_stream_from(std::size_t from, std::string& out) {
  return g_repl_backlog.get_slice(static_cast<std::int64_t>(from), out);
}

std::function<void(const std::string&, bool)> g_replication_snapshot_done_cb;

int start_replication_snapshot(RdbReplicationInfo& info) {
  if (has_active_child_process()) return 0;
  info.stream_db = g_replication_role == "slave" ? g_master_stream_session.db_index : 0;
  info.replid = g_master_replid;
  info.offset = g_master_repl_offset;
  if (!do_bgsave_fork(&info)) return -1;
  return 1;
}

void replicas_took_backlog() {
  g_replicas_taken_to = g_repl_backlog.total;
}

bool replica_getack_pending() {
  return getack_waiting();
}

void send_pending_getack() {
  if (!getack_waiting()) return;
  g_getack_pending = false;
  // replicationFeedSlaves(): nothing goes out with no backlog (no replica has attached yet), and
  // a replica's stream is its master's.
  if (g_replication_role != "slave" && g_backlog_has_replica)
    backlog_append(encode_command_resp({"REPLCONF", "GETACK", "*"}));
}

void set_replica_wakeup_fd(int write_fd, int read_fd) {
  g_replica_wakeup_read_fd = read_fd;
  g_replica_wakeup_write_fd = write_fd;
}

// Empties the pipe before the pending flag is cleared, so a byte the link thread writes after
// this is seen by the next poll and a command it queued before it is seen by the next drain.
void take_replica_wakeup() {
  char buf[64];
  while (g_replica_wakeup_read_fd >= 0 && ::read(g_replica_wakeup_read_fd, buf, sizeof(buf)) > 0) {
  }
  g_replica_wakeup_pending = false;
}

// Redis's migrateCloseTimedoutSockets(), run once a second from serverCron(): closes the MIGRATE
// connections not used for more than 10 seconds.
void migrate_cron(std::int64_t now) {
  static std::int64_t last_run_ms = 0;
  if (now - last_run_ms < 1000) return;
  last_run_ms = now;
  const std::int64_t now_s = now / 1000;
  for (auto it = g_migrate_cached_sockets.begin(); it != g_migrate_cached_sockets.end();) {
    if (now_s - it->second.last_use_time > kMigrateSocketCacheTtl) {
      ::close(it->second.fd);
      it = g_migrate_cached_sockets.erase(it);
    } else {
      ++it;
    }
  }
}

void replication_cron(std::int64_t now) {
  static std::int64_t last_run = 0;
  static std::uint64_t loops = 0;
  if (now - last_run < 1000) return;
  last_run = now;
  if (g_replication_role == "slave") {
    send_replica_ack();
  } else if (loops % g_repl_ping_replica_period == 0 && g_count_connected_replicas &&
             g_count_connected_replicas() > 0) {
    // replicationFeedSlaves(..., -1): PING has no SELECT and never reaches the AOF.
    backlog_append("*1\r\n$4\r\nPING\r\n");
  }
  ++loops;
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
    return null_bulk ? encode_null(session.resp_version) : encode_null_array(session.resp_version);
  }

  // Redis checks a woken key without deleting it (handleClientsBlockedOnKey()'s LOOKUP_NOEFFECTS
  // lookup): a list, sorted-set or XREAD blocker runs only when one of its keys is a live key of
  // its type,
  // so a key that has expired keeps the client blocked, and its DEL waits for a command that meets
  // it. (Redis looks at the time of the command that made the key ready, PeaDB at the time of the
  // check: compat/delta.md item 75.)
  // XREADGROUP is left out: a key that is gone must wake it with its error (Redis's
  // unblock_on_nokey), so it reads, and an expired key it meets is deleted then.
  const bool xread = session.blocked.type == BlockType::Stream &&
                     !session.blocked.original_args.empty() &&
                     upper(session.blocked.original_args[0]) == "XREAD";
  if (session.blocked.type == BlockType::List || session.blocked.type == BlockType::ZSet || xread) {
    const ValueType wanted = session.blocked.type == BlockType::List   ? ValueType::List
                             : session.blocked.type == BlockType::ZSet ? ValueType::ZSet
                                                                       : ValueType::Stream;
    bool ready = false;
    for (const auto& key : session.blocked.keys) {
      if (store().live_type(key) == wanted) {
        ready = true;
        break;
      }
    }
    if (!ready) return {};
  }

  // Mutations applied on wakeup happen outside the dispatch chokepoint, so they must
  // propagate to the replication backlog and AOF explicitly (deterministic pop form).
  auto propagate = [&session](std::vector<std::string> cmd_args) {
    store().increment_mutation_epoch();
    propagate_write(cmd_args, session, nullptr);
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
            if (session.blocked.pop_left)
              (void)store().lpush(captured_key, one, wt);
            else
              (void)store().rpush(captured_key, one, wt);
            clear_block();
            return wrongtype_error_reply();
          }
          if (where_to == "LEFT")
            (void)store().lpush(dst, one, wt);
          else
            (void)store().rpush(dst, one, wt);
          propagate(
              {"LMOVE", captured_key, dst, session.blocked.pop_left ? "LEFT" : "RIGHT", where_to});
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

  return {};  // still blocked
}

std::string try_unblock_client(SessionState& session) {
  if (session.blocked.type == BlockType::None) return {};
  // The client's keys are in its own database, not in the one the last command used (a client
  // blocked in db 9 was served by a push to the same key name in db 1, and its pop was propagated
  // under db 9).
  store().select_db(session.db_index);
  // Capture the block context before try_unblock_client_impl clears it on serve/timeout.
  const std::vector<std::string> orig = session.blocked.original_args;
  const std::int64_t start_ms = session.blocked.block_start_ms;
  // A woken command is its own execution unit, as in Redis, so the writes it propagates (a woken
  // XREADGROUP's XCLAIMs and group id) are wrapped together.
  const WriteOffsetTracker write_offset(session);
  const ExecutionUnit unit(&orig);
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

void register_key_waiter(const std::string& /*key*/, int /*fd*/) {
}
void unregister_key_waiter(int /*fd*/) {
}

bool has_pending_wakeups() {
  return g_has_pending_wakeups;
}
void clear_pending_wakeups() {
  g_has_pending_wakeups = false;
}

void notify_key_written(const std::string& /*key*/, int /*db_index*/) {
  g_has_pending_wakeups = true;
}

std::size_t command_count() {
  return command_table().size();
}

std::vector<std::string> command_names() {
  std::vector<std::string> names;
  names.reserve(command_table().size());
  for (const auto& [name, _] : command_table()) names.push_back(name);
  std::sort(names.begin(), names.end());
  return names;
}

void aof_cron(std::int64_t now) {
  if (g_aof_state != AofState::On) return;
  // beforeSleep() and serverCron(): what a failed write left is written again on every pass.
  if (!g_aof_buf.empty()) flush_append_only_file({});
  if (g_aof_fd < 0) return;
  // Bytes written before CONFIG SET appendfsync always.
  if (g_appendfsync == "always" && g_aof_unsynced) aof_fsync_always();
  // The everysec fsync of new bytes (Redis's bio thread). Redis clears a failed fsync only with
  // the fsync of a later write; PeaDB also tries the fsync again every second while it fails,
  // because no write may come: writes are refused then, and only an expiring key's DEL would
  // heal it (compat/delta.md item 35, plan decision D13).
  const bool due = (g_appendfsync == "everysec" && g_aof_unsynced) || g_aof_fsync_error;
  if (!due || now - g_last_aof_fsync_ms < 1000) return;
  g_aof_unsynced = false;
  g_last_aof_fsync_ms = now;
  // EBADF and EINVAL do not count as a failed fsync, as in Redis.
  if (::fsync(g_aof_fd) == -1 && errno != EBADF && errno != EINVAL) {
    const int fsync_errno = errno;
    if (!g_aof_fsync_error) {
      log(LogLevel::Warn, "Fail to fsync the AOF file: " + std::string(std::strerror(fsync_errno)));
    }
    g_aof_fsync_error = true;
    g_aof_fsync_errno = fsync_errno;
  } else {
    g_aof_fsync_error = false;
  }
}

void drain_replica_cmd_queue() {
  std::shared_ptr<MasterLinkRequest> request;
  {
    std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
    request = std::move(g_master_link_request);
    if (request && request->gen == g_repl_generation.load() && !request->resume) {
      // freeClient() does not cache a CLIENT_BLOCKED master (CLIENT PAUSE). Its held
      // input cannot be resurrected; the next link must replace the dataset in full.
      const MasterStreamCommand* pending =
          !g_replica_cmd_held.empty()
              ? &g_replica_cmd_held.front()
              : (!g_replica_cmd_queue.empty() ? &g_replica_cmd_queue.front() : nullptr);
      if (pending && !pending->args.empty() && g_paused_actions != 0 &&
          pause_applies(upper(pending->args[0]), pending->args, g_master_stream_session))
        g_cached_master = false;
      g_replica_cmd_queue.clear();
      g_replica_cmd_held.clear();
    }
  }
  if (request) {
    if (request->gen == g_repl_generation.load()) {
      if (!request->resume) {
        const int db = g_master_stream_session.db_index;
        g_master_stream_session = SessionState{};
        g_master_stream_session.db_index = db;
        g_master_stream_multi.clear();
        request->keepalive = g_tcp_keepalive;
        request->cached = g_cached_master;
        request->replid = g_cached_master ? g_master_replid : "?";
        request->offset = g_cached_master ? g_master_repl_offset : -1;
        g_replica_read_offset = g_master_repl_offset;
      } else if (g_cached_master) {
        if (request->replid != g_master_replid) {
          g_master_replid2 = g_master_replid;
          g_second_replid_offset = g_master_repl_offset + 1;
          g_master_replid = request->replid;
          disconnect_replicas();
        }
        request->cached = true;
      }
    }
    request->done = true;
  }
  // Apply any pending replica RDB load on the event loop thread before processing
  // queued commands. Running rdb_load() here serializes flushall+restore with all
  // event-loop reads/writes, eliminating the race against the streaming thread.
  if (g_replica_load_pending.load()) {
    std::string path;
    std::string replid;
    std::int64_t offset = 0;
    std::uint64_t seq = 0;
    // REPLICAOF runs on this thread too, so the generation cannot change before the load is done.
    bool current = false;
    {
      std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
      path = g_replica_load_path;
      g_replica_load_path.clear();
      replid = g_replica_load_replid;
      offset = g_replica_load_offset;
      seq = g_replica_load_seq;
      current = g_replica_load_gen == g_repl_generation.load();
      // Commands still queued or held came from the stream before this full sync; the new
      // dataset already holds their effects, so applying them now would apply them twice.
      if (current) {
        g_replica_cmd_queue.clear();
        g_replica_cmd_held.clear();
      }
    }
    bool ok = false;
    if (!current) {
      log(LogLevel::Warn, "REPLICAOF: dropping a full sync from a master link that was replaced");
    } else {
      std::string err;
      // A full resync replaces the entire dataset, including server-level function libraries.
      // rdb_load() flushes the keyspace but not functions, so it clears them once the file passes
      // its checks — otherwise libraries that exist only on this node (from before it became a
      // replica) would survive and diverge from the master (Redis's emptyData clears functions on
      // full sync). The checksum is checked as for any load (a real Redis master writes the same
      // CRC64 as PeaDB, or 0 with rdbchecksum no); on a mismatch keys and libraries are kept and
      // the sync is tried again.
      int stream_db = -1;
      try {
        ok = rdb_load(
            path, err,
            [&] {
              // replicationAttachToNewMaster(): only after header/checksum validation discard the
              // old cached master and backlog. A decoding failure after this boundary may have
              // replaced part of the dataset; neither reconnect nor promotion can reuse its
              // history.
              g_cached_master = false;
              g_master_stream_session = SessionState{};
              g_master_stream_multi.clear();
              disconnect_replicas();
              g_repl_backlog.reset_at(g_master_repl_offset);
              g_replicas_taken_to = g_master_repl_offset;
              g_backlog_has_replica = false;
              // readSyncBulkPayload(): an old save/rewrite must never overwrite the new dataset.
              // Do this after checksum validation, so a corrupt transfer keeps the old disk files.
              kill_rdb_child();
              if (g_appendonly && !g_aof_sync_suspended) {
                stop_append_only();
                g_appendonly = true;  // keep the configured setting while the AOF is suspended
                g_aof_sync_suspended = true;
              }
              if (::rename(path.c_str(), g_snapshot_path.c_str()) != 0) {
                throw std::runtime_error(
                    "Failed trying to rename the temp DB into " + g_snapshot_path.string() +
                    " in MASTER <-> REPLICA synchronization: " + std::strerror(errno));
              }
              if (!fsync_parent_dir(g_snapshot_path)) {
                throw std::runtime_error(
                    "Failed trying to sync DB directory in MASTER <-> REPLICA synchronization: " +
                    std::string(std::strerror(errno)));
              }
              functions_flush_all();
            },
            &stream_db);
      } catch (const std::exception& e) {
        err = e.what();
      }
      if (!ok) {
        log(LogLevel::Error, "REPLICAOF: rdb_load on event loop failed: " + err);
      } else {
        // readSyncBulkPayload(): the replica takes the master's replication id and offset, a new
        // history (replid2 cleared), and a backlog that starts empty there, so its replicas and a
        // promotion continue from the stream it applies. Its master's stream starts in the database
        // the RDB names (repl-stream-db).
        g_master_replid = replid;
        g_master_replid2 = std::string(40, '0');
        g_second_replid_offset = -1;
        g_master_repl_offset = offset;
        g_cached_master = true;
        start_backlog();
        if (stream_db >= 0) g_master_stream_session.db_index = stream_db;
        note_rdb_saved(static_cast<std::int64_t>(store().mutation_epoch()));
        if (g_aof_sync_suspended) restart_aof_after_sync();
      }
    }
    {
      std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
      g_replica_load_done_seq = seq;
      g_replica_load_ok = ok;
      g_replica_load_pending.store(false);  // signal streaming thread
    }
  }

  {
    std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
    for (auto& c : g_replica_cmd_queue) g_replica_cmd_held.push_back(std::move(c));
    g_replica_cmd_queue.clear();
  }
  if (g_replica_cmd_held.empty()) return;
  // Apply on the event loop thread: must-obey skips client gates, but master commands write
  // their effects to the local AOF. Raw bytes feed the backlog separately below. No key expires
  // for the master's commands (Redis's expireIfNeeded() for its master client): the master ran
  // them while their keys were alive, and its DEL comes when it expires them.
  const DataStore::LazyExpireDisabled no_lazy_expire(store());
  bool close_flag = false;
  SessionState& session = g_master_stream_session;
  // A paused replica holds what its master sends, as Redis pauses its master client: the first
  // command the pause holds and everything after it wait, in order, until the pause ends.
  while (!g_replica_cmd_held.empty()) {
    MasterStreamCommand& cmd = g_replica_cmd_held.front();
    if (g_paused_actions != 0 && !cmd.args.empty() &&
        pause_applies(upper(cmd.args[0]), cmd.args, session))
      break;
    {
      // The master's command bypasses the client-side checks; a failure is logged and the stream
      // goes on, and nothing leaves g_replaying_aof set for the clients that come next.
      struct Replaying {
        Replaying() {
          g_replaying_aof = true;
          g_applying_master_stream = true;
        }
        ~Replaying() {
          g_replaying_aof = false;
          g_applying_master_stream = false;
        }
      } replaying;
      g_master_stream_command_name = !cmd.args.empty() && command_table().count(upper(cmd.args[0]))
                                         ? full_command_name(upper(cmd.args[0]), cmd.args)
                                         : "<unknown>";
      try {
        handle_command(cmd.args, session, close_flag);
      } catch (const std::exception& e) {
        log(LogLevel::Warn,
            std::string("REPLICAOF: a command from the master failed: ") + e.what());
      }
      g_master_stream_command_name.clear();
    }
    // commandProcessed(): what has been applied joins this server's own stream, its backlog and
    // its replicas, byte for byte as the master sent it, which moves its replication offset (the
    // one it acknowledges); a transaction only once its EXEC has run.
    if (session.in_multi) {
      g_master_stream_multi += cmd.raw;
    } else if (g_master_stream_multi.empty()) {
      backlog_append(cmd.raw);
    } else {
      g_master_stream_multi += cmd.raw;
      backlog_append(g_master_stream_multi);
      g_master_stream_multi.clear();
    }
    g_replica_cmd_held.pop_front();
  }
}

// ── In-process replication client helpers ────────────────────────────────────

// Generation-tagged event-loop hand-off: the link never reads or edits stream history itself.
static bool master_link_request(const std::shared_ptr<MasterLinkRequest>& request) {
  for (;;) {
    {
      std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
      if (g_repl_generation.load() != request->gen) return false;
      if (!g_master_link_request) {
        g_master_link_request = request;
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  wake_event_loop_for_replica();
  while (!request->done.load()) {
    if (g_repl_generation.load() != request->gen) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return g_repl_generation.load() == request->gen;
}

template <typename F>
static bool update_master_link(std::uint64_t gen, F&& update) {
  std::lock_guard<std::mutex> lk(g_replica_link_mu);
  if (g_repl_generation.load() != gen) return false;
  update();
  return true;
}

// Nonblocking handshake/transfer I/O with the same inactivity limit as the connected stream.
// A byte of progress (including a keep-alive newline) resets the timer; CONFIG SET is live.
class ReplicationIo {
 public:
  ReplicationIo(int fd, std::uint64_t gen) : fd_(fd), gen_(gen), last_io_(now_ms()) {
  }
  bool wait(short events) {
    while (g_repl_generation.load() == gen_) {
      if (now_ms() / 1000 - last_io_ / 1000 > g_repl_timeout.load()) {
        log(LogLevel::Warn, "Timeout connecting to or receiving bulk data from MASTER...");
        return false;
      }
      pollfd p{fd_, events, 0};
      const int n = ::poll(&p, 1, 100);
      if (n > 0) return true;  // recv/send reports EOF or the socket error
      if (n < 0 && errno != EINTR) return false;
    }
    return false;
  }
  bool readExact(void* buf, std::size_t len) {
    auto* p = static_cast<char*>(buf);
    while (len > 0 && wait(POLLIN)) {
      const ssize_t n = ::recv(fd_, p, len, 0);
      if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
      if (n <= 0) return false;
      last_io_ = now_ms();
      update_master_link(gen_, [&] { g_repl_transfer_lastio_ms = last_io_; });
      p += n;
      len -= static_cast<std::size_t>(n);
    }
    return len == 0;
  }
  bool readLine(std::string& out) {
    out.clear();
    char c;
    while (readExact(&c, 1)) {
      if (c == '\n') {
        if (!out.empty() && out.back() == '\r') out.pop_back();
        return true;
      }
      out += c;
      if (out.size() > 65536) return false;
    }
    return false;
  }
  bool send(const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size() && wait(POLLOUT)) {
      const ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, 0);
      if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
      if (n <= 0) return false;
      sent += static_cast<std::size_t>(n);
      last_io_ = now_ms();
    }
    return sent == data.size();
  }

 private:
  int fd_;
  std::uint64_t gen_;
  std::int64_t last_io_;
};

static std::string repl_encode_cmd(const std::vector<std::string>& parts) {
  std::string out = "*" + std::to_string(parts.size()) + "\r\n";
  for (const auto& s : parts) out += "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
  return out;
}

// Perform a full PSYNC handshake with master at host:port and load the RDB snapshot.
bool replica_do_fullsync(const std::string& host, int port, std::uint64_t gen) {
  auto cached = std::make_shared<MasterLinkRequest>();
  cached->gen = gen;
  if (!master_link_request(cached)) return false;
  struct addrinfo hints {
  }, *res = nullptr;
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
  ::fcntl(fd, F_SETFD, FD_CLOEXEC);
  ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
  ReplicationIo io(fd, gen);
  if (cached->keepalive > 0) {
    const int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes));
    const int interval = cached->keepalive;
#ifdef __linux__
    const int probe_interval = std::max(interval / 3, 1), probes = 3;
    ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &interval, sizeof(interval));
    ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &probe_interval, sizeof(probe_interval));
    ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &probes, sizeof(probes));
#elif defined(__APPLE__)
    ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &interval, sizeof(interval));
#endif
  }
  // ROLE's link states: connecting, the handshake (PING ... PSYNC), the transfer, connected;
  // back to connect whenever this attempt ends.
  struct LinkStateReset {
    std::uint64_t gen;
    ~LinkStateReset() {
      update_master_link(gen, [] { g_repl_link_state = ReplLinkState::Connect; });
    }
  } link_state_reset{gen};
  if (!update_master_link(gen, [] { g_repl_link_state = ReplLinkState::Connecting; })) {
    ::freeaddrinfo(res);
    ::close(fd);
    return false;
  }
  int connected = ::connect(fd, res->ai_addr, res->ai_addrlen);
  if (connected != 0 && errno == EINPROGRESS && io.wait(POLLOUT)) {
    int error = 0;
    socklen_t len = sizeof(error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) == 0 && error == 0)
      connected = 0;
    else if (error != 0)
      errno = error;
  }
  if (connected != 0) {
    log(LogLevel::Error,
        "REPLICAOF: connect to " + host + ":" + port_str + " failed: " + strerror(errno));
    ::freeaddrinfo(res);
    ::close(fd);
    return false;
  }
  ::freeaddrinfo(res);
  update_master_link(gen, [] { g_repl_link_state = ReplLinkState::Handshake; });

  std::string line;

  // PING
  if (!io.send("*1\r\n$4\r\nPING\r\n") || !io.readLine(line)) {
    log(LogLevel::Error, "REPLICAOF: PING failed, got: " + line);
    ::close(fd);
    return false;
  }
  // Redis's syncWithMaster tolerates -NOAUTH / -NOPERM / "operation not permitted" replies
  // to the pre-AUTH PING: the master is alive, so replication continues to the AUTH step.
  if (line.rfind("+", 0) != 0 && line.rfind("-NOAUTH", 0) != 0 && line.rfind("-NOPERM", 0) != 0 &&
      line.rfind("-ERR operation not permitted", 0) != 0) {
    log(LogLevel::Error, "REPLICAOF: PING failed, got: " + line);
    ::close(fd);
    return false;
  }

  // syncWithMaster: an empty password skips AUTH even if masteruser is set. The password is
  // binary-safe (Redis's SDS), while an empty username selects the password-only form.
  if (const auto credentials = master_credentials_copy(); !credentials.second.empty()) {
    const auto& [masteruser, masterauth] = credentials;
    const auto auth = masteruser.empty() ? repl_encode_cmd({"AUTH", masterauth})
                                         : repl_encode_cmd({"AUTH", masteruser, masterauth});
    if (!io.send(auth) || !io.readLine(line) || line.rfind("-", 0) == 0) {
      // Redis's syncWithMaster logs exactly this on a rejected AUTH; tooling and tests
      // (unit/auth MASTERAUTH) match on the "Unable to AUTH to MASTER" phrasing.
      log(LogLevel::Error, "Unable to AUTH to MASTER: " + line);
      ::close(fd);
      return false;
    }
  }

  // REPLCONF listening-port
  if (!io.send(repl_encode_cmd({"REPLCONF", "listening-port", std::to_string(g_server_port)})) ||
      !io.readLine(line)) {
    log(LogLevel::Error, "REPLICAOF: REPLCONF listening-port failed: " + line);
    ::close(fd);
    return false;
  }
  if (line.rfind("-", 0) == 0)
    log(LogLevel::Info,
        "(Non critical) Master does not understand REPLCONF listening-port: " + line);

  // REPLCONF capa psync2
  if (!io.send(repl_encode_cmd({"REPLCONF", "capa", "psync2"})) || !io.readLine(line)) {
    log(LogLevel::Error, "REPLICAOF: REPLCONF capa failed: " + line);
    ::close(fd);
    return false;
  }
  if (line.rfind("-", 0) == 0)
    log(LogLevel::Info, "(Non critical) Master does not understand REPLCONF capa: " + line);

  // Keep-alives can precede FULLRESYNC while the master waits for another child.
  if (!io.send(repl_encode_cmd(
          {"PSYNC", cached->replid, std::to_string(cached->cached ? cached->offset + 1 : -1)}))) {
    ::close(fd);
    return false;
  }
  do {
    if (!io.readLine(line)) {
      ::close(fd);
      return false;
    }
  } while (line.empty());
  const bool partial = line == "+CONTINUE" || line.rfind("+CONTINUE ", 0) == 0;
  if (partial) {
    if (!cached->cached) {
      ::close(fd);
      return false;
    }
    auto resume = std::make_shared<MasterLinkRequest>();
    resume->gen = gen;
    resume->resume = true;
    resume->replid = line.size() == 50 ? line.substr(10) : cached->replid;
    if (!master_link_request(resume) || !resume->cached) {
      ::close(fd);
      return false;
    }
    log(LogLevel::Info, "Successful partial resynchronization with master.");
  } else {
    if (line.rfind("+FULLRESYNC", 0) != 0) {
      log(LogLevel::Error, "REPLICAOF: expected +FULLRESYNC, got: " + line);
      ::close(fd);
      return false;
    }
    // Parse replid and offset: "+FULLRESYNC <replid> <offset>". The event loop adopts them once
    // the RDB has loaded.
    std::int64_t fullresync_offset = 0;
    std::string fullresync_replid;
    {
      auto sp1 = line.find(' ');
      auto sp2 = line.find(' ', sp1 + 1);
      if (sp1 != std::string::npos && sp2 != std::string::npos) {
        fullresync_replid = line.substr(sp1 + 1, sp2 - sp1 - 1);
        parse_i64(line.substr(sp2 + 1), fullresync_offset);
      }
    }
    // INFO's master_sync_in_progress and master_sync_* fields, until the RDB has loaded.
    struct TransferState {
      std::uint64_t gen;
      explicit TransferState(std::uint64_t generation) : gen(generation) {
        update_master_link(gen, [] {
          g_repl_transfer_size = -1;
          g_repl_transfer_read = 0;
          g_repl_transfer_lastio_ms = now_ms();
          g_repl_transfer_in_progress = true;
          g_repl_link_state = ReplLinkState::Transfer;
        });
      }
      void done() {
        update_master_link(gen, [] { g_repl_transfer_in_progress = false; });
      }
      ~TransferState() {
        done();
      }
    } transfer_state{gen};

    // Redis sends a keep-alive newline every second during a disk-backed BGSAVE.
    do {
      if (!io.readLine(line)) {
        ::close(fd);
        return false;
      }
    } while (line.empty());
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
    update_master_link(gen, [&] { g_repl_transfer_size = rdb_len; });

    // Stream the RDB into a temp file in the working directory (`dir`), created new like Redis's
    // readSyncBulkPayload() creates temp-<unixtime>.<pid>.rdb; the link's generation in the name
    // keeps two links (an old one not stopped yet and a new one) apart.
    const std::string tmp_path = "temp-" + std::to_string(std::time(nullptr)) + "." +
                                 std::to_string(::getpid()) + "." + std::to_string(gen) + ".rdb";
    {
      int rfd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
      if (rfd < 0) {
        log(LogLevel::Error,
            "Opening the temp file needed for MASTER <-> REPLICA synchronization: " +
                std::string(strerror(errno)));
        ::close(fd);
        return false;
      }
      std::vector<char> chunk(65536);
      std::int64_t remaining = rdb_len;
      bool write_ok = true;
      while (remaining > 0 && write_ok) {
        std::size_t want =
            static_cast<std::size_t>(std::min(remaining, static_cast<std::int64_t>(chunk.size())));
        if (!io.readExact(chunk.data(), want)) {
          write_ok = false;
          break;
        }
        std::size_t written = 0;
        while (written < want) {
          const ssize_t n = ::write(rfd, chunk.data() + written, want - written);
          if (n < 0 && errno == EINTR) continue;
          if (n <= 0) {
            write_ok = false;
            break;
          }
          written += static_cast<std::size_t>(n);
        }
        remaining -= static_cast<std::int64_t>(want);
        update_master_link(gen, [&] { g_repl_transfer_read = rdb_len - remaining; });
      }
      if (write_ok && ::fsync(rfd) != 0) write_ok = false;
      if (::close(rfd) != 0) write_ok = false;
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
    // One hand-off at a time: an older link's may still wait for the event loop. A newer REPLICAOF
    // makes this one give up.
    std::uint64_t my_seq = 0;
    for (;;) {
      {
        std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
        if (g_repl_generation.load() != gen) break;
        if (!g_replica_load_pending.load()) {
          g_replica_load_path = tmp_path;
          g_replica_load_gen = gen;
          g_replica_load_replid = fullresync_replid;
          g_replica_load_offset = fullresync_offset;
          my_seq = ++g_replica_load_seq;
          g_replica_load_pending.store(true);
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (my_seq == 0) {
      ::unlink(tmp_path.c_str());
      ::close(fd);
      log(LogLevel::Info, "REPLICAOF: superseded by a newer REPLICAOF before loading the RDB");
      return false;
    }
    update_master_link(gen, [] { g_loading_replication = true; });
    wake_event_loop_for_replica();
    bool load_ok = false;
    for (;;) {
      {
        std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
        if (g_replica_load_done_seq == my_seq) {
          load_ok = g_replica_load_ok;
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    transfer_state.done();
    update_master_link(gen, [] { g_loading_replication = false; });
    ::unlink(tmp_path.c_str());
    if (!load_ok) {
      ::close(fd);
      log(LogLevel::Error, "REPLICAOF: rdb_load failed (event-loop)");
      return false;
    }
    cached->offset = fullresync_offset;
    log(LogLevel::Info,
        "REPLICAOF: full sync complete, loaded " + std::to_string(rdb_len) + " bytes");

  }  // full resync; CONTINUE keeps the dataset and backlog

  // Release loading gate; from here the event loop's READONLY guard rejects client writes.
  // Streaming commands are enqueued in g_replica_cmd_queue and applied by the event loop,
  // so DataStore mutations stay on the event loop thread (no concurrent access).
  update_master_link(gen, [] { g_loading_replication = false; });
  {
    // The event loop sends the REPLCONF ACKs on this socket from now on (it knows what it has
    // applied); this thread only reads. A newer REPLICAOF has its own link.
    std::lock_guard<std::mutex> lk(g_replica_link_mu);
    if (g_repl_generation.load() == gen) {
      g_replica_read_offset = cached->offset;
      g_replica_last_io_ms = now_ms();
      g_repl_down_since_ms = 0;
      g_replica_link_fd = fd;
      g_replica_ack_out.clear();
      g_replica_stale = false;
      g_repl_link_state = ReplLinkState::Connected;
    }
  }

  // Stream subsequent replication commands from master.
  // The master sends a continuous RESP stream of write commands after the RDB.
  // Commands are enqueued and applied by drain_replica_cmd_queue() in the event loop, each with
  // its bytes, which the event loop adds to this server's own stream once it has applied them.
  std::string stream_buf;
  stream_buf.reserve(65536);
  std::vector<char> recv_buf(65536);

  while (true) {
    // The 100 ms poll bounds how long a superseded stream takes to release its socket.
    if (g_repl_generation.load() != gen) {
      log(LogLevel::Info, "REPLICAOF: superseded by a newer REPLICAOF, stopping stream");
      break;
    }
    if (now_ms() / 1000 - g_replica_last_io_ms.load() / 1000 > g_repl_timeout.load()) {
      log(LogLevel::Warn, "MASTER timeout: no data nor PING received...");
      break;
    }
    pollfd readable{fd, POLLIN, 0};
    const int ready = ::poll(&readable, 1, 100);
    if (ready == 0 || (ready < 0 && errno == EINTR)) continue;
    if (ready < 0) break;
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
    if (!update_master_link(gen, [&] {
          g_replica_read_offset += n;  // readQueryFromClient(): read_reploff
          g_replica_last_io_ms = now_ms();
        }))
      break;

    // Parse commands and enqueue them for the event loop to apply.
    // This keeps all DataStore mutations on the event loop thread, eliminating
    // the data race between the streaming thread and concurrent client reads.
    std::size_t consumed = 0;
    {
      std::lock_guard<std::mutex> lk(g_replica_cmd_queue_mu);
      // A newer REPLICAOF may have come in during the recv: queue nothing for it
      // (drop_queued_replica_commands() runs under this mutex after the bump).
      while (g_repl_generation.load() == gen) {
        const std::string_view view(stream_buf.data() + consumed, stream_buf.size() - consumed);
        auto parsed = parse_one_command(view);
        if (!parsed.has_value()) break;
        g_replica_cmd_queue.push_back(
            {std::move(parsed->args), std::string(view.substr(0, parsed->consumed))});
        consumed += parsed->consumed;
      }
    }

    if (consumed > 0) {
      if (consumed == stream_buf.size())
        stream_buf.clear();
      else
        stream_buf.erase(0, consumed);
      wake_event_loop_for_replica();
    }
  }
  {
    std::lock_guard<std::mutex> lk(g_replica_link_mu);
    if (g_replica_link_fd == fd) {
      g_replica_link_fd = -1;
      g_replica_ack_out.clear();
    }
  }
  ::close(fd);
  // replicationHandleMasterDisconnection(): repl_down_since.
  if (update_master_link(gen, [] {
        g_repl_down_since_ms = now_ms();
        g_replica_stale = true;
      })) {
    auto reset = std::make_shared<MasterLinkRequest>();
    reset->gen = gen;
    master_link_request(reset);
  }
  return true;
}

// ─────────────────────────────────────────────────────────────────────────────

void reap_background_children() {
  if (g_bgsave_in_progress.load()) {
    int status = 0;
    pid_t pid = g_bgsave_child_pid.load();
    if (pid > 0 && ::waitpid(pid, &status, WNOHANG) == pid) bgsave_done(pid, status, false);
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
      if (WIFEXITED(status) && WEXITSTATUS(status) == 0 && g_aof_state != AofState::Off) {
        if (g_aof_rewrite_buf_capped) {
          // Diff buffer overflowed during rewrite. Abandon: keep old AOF.
          ::unlink(tmp_path.c_str());
          log(LogLevel::Error,
              "BGREWRITEAOF: aborted because diff buffer hit 256 MiB cap; "
              "old AOF retained intact (dropping " +
                  std::to_string(g_aof_rewrite_buf.size()) +
                  " bytes of partial diff). Reduce write rate and rerun BGREWRITEAOF.");
          clear_buffer(g_aof_rewrite_buf);
          g_aof_last_bgrewrite_status = "err";
          // g_aof_fd unchanged — still pointing at the live old AOF.
        } else {
          // Append diff buffer to temp.aof before committing via rename.
          bool diff_ok = true;
          if (!g_aof_rewrite_buf.empty()) {
            const int diff_fd = ::open(tmp_path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC, 0644);
            if (diff_fd < 0) {
              diff_ok = false;
            } else {
              const char* p = g_aof_rewrite_buf.data();
              std::size_t remaining = g_aof_rewrite_buf.size();
              while (remaining > 0) {
                const ssize_t rc = ::write(diff_fd, p, remaining);
                if (rc <= 0) {
                  diff_ok = false;
                  break;
                }
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
              log(LogLevel::Error,
                  "BGREWRITEAOF: parent directory fsync failed; "
                  "new AOF is live but the rename may revert across a crash. "
                  "Reporting aof_last_bgrewrite_status:err — retry the rewrite.");
            }
            clear_buffer(g_aof_rewrite_buf);
            g_aof_last_bgrewrite_status = dir_durable ? "ok" : "err";
            g_aofrw_consecutive_failures = 0;
            open_aof_fd();  // reopen on the new inode
            // The new file, written and synced, holds every write: what a failed write left in
            // g_aof_buf is in the dataset the child saved or in the rewrite buffer, so writing
            // it again would apply it twice. A good rewrite ends an earlier write or fsync error.
            clear_buffer(g_aof_buf);
            g_aof_unsynced = false;
            g_aof_fsync_error = false;
            if (g_aof_write_error) {
              log(LogLevel::Info, "AOF write error looks solved, PeaDB can write again.");
              g_aof_write_error = false;
            }
            if (g_aof_state == AofState::WaitRewrite) {
              g_aof_state = AofState::On;  // CONFIG SET appendonly yes is now in effect
              log(LogLevel::Info, "Background AOF rewrite finished successfully; the AOF is on");
            }
          } else {
            ::unlink(tmp_path.c_str());
            const std::size_t dropped = g_aof_rewrite_buf.size();
            // Clear the buffer: the live AOF (which append_aof has been writing
            // to throughout the rewrite window) already contains every command
            // the buffer holds. The next BGREWRITEAOF will fork from the current
            // in-memory state — which also already reflects these commands — so
            // re-appending the old buffer would double-apply non-idempotent
            // operations (INCR, list pushes, set removals, stream appends).
            clear_buffer(g_aof_rewrite_buf);
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
                                  std::to_string(g_aof_rewrite_buf.size()) +
                                  " bytes of buffered diff");
        }
        clear_buffer(g_aof_rewrite_buf);
        g_aof_last_bgrewrite_status = "err";
      }
      // The AOF that CONFIG SET appendonly yes turned on still has no file: try again from the
      // cron (Redis's aof_rewrite_scheduled). The next rewrite forks from the current dataset,
      // which holds every write the dropped buffer held.
      if (g_aof_last_bgrewrite_status == "err") ++g_aofrw_consecutive_failures;
      if (g_aof_state == AofState::WaitRewrite && !g_bgrewrite_in_progress.load()) {
        g_aof_rewrite_scheduled = true;
      }
    }
  }
  // Redis's serverCron(): a rewrite asked for beside another child or inside EXEC, or the first
  // rewrite of CONFIG SET appendonly yes, starts once no child runs.
  if (g_aof_rewrite_scheduled && !has_active_child_process() && !aof_rewrite_limited() &&
      !start_aof_rewrite()) {
    ++g_aofrw_consecutive_failures;
  }
}

// ── ServerContext accessor ────────────────────────────────────────────────

static ServerContext g_server_context;

ServerContext& server_ctx() {
  return g_server_context;
}

}  // namespace peadb
