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

#include "config.hpp"

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
  std::vector<std::string> keys;           // keys being watched
  bool pop_left = true;                    // for list: BLPOP vs BRPOP
  bool zpop_max = false;                   // for zset: BZPOPMIN vs BZPOPMAX
  std::int64_t deadline_ms = 0;            // 0 = infinite
  std::uint64_t seq = 0;                   // FIFO serving order (set when the block registers)
  std::vector<std::string> original_args;  // original command args for context
  std::int64_t block_start_ms = 0;         // wall-clock when the command first blocked (0 = unset);
                                    // slowlog logs the command only on unblock, with this span
};

struct WaitState {
  std::int64_t target_offset = 0;  // master offset at WAIT issue time
  std::int64_t deadline_ms = 0;    // absolute deadline; 0 = no timeout
  std::int64_t min_replicas = 0;   // number of replicas to wait for
};

enum class ReplicaSyncState { Online, WaitSnapshot, SaveSnapshot, SendSnapshot };

struct SessionState {
  RespVersion resp_version = RespVersion::Resp2;
  int db_index = 0;
  bool in_multi = false;
  bool multi_dirty = false;
  std::vector<std::vector<std::string>> queued;
  // Redis's mstate.cmd_flags and cmd_inv_flags: the OR of the queued commands' flags (kRedisCmd*,
  // as getCommandFlags() gave them when each was queued) and of their complements, by which
  // processCommand() checks EXEC. Reset by MULTI.
  std::uint64_t multi_flags = 0;
  std::uint64_t multi_inv_flags = 0;
  std::optional<std::uint64_t> watched_epoch;
  std::unordered_map<std::string, std::optional<std::string>> watched_digests;
  bool asking = false;
  bool replica_stream = false;
  ReplicaSyncState repl_sync_state = ReplicaSyncState::Online;
  bool repl_legacy_sync = false;
  std::size_t repl_index = 0;
  bool repl_send_select = false;
  std::int64_t repl_ack_offset = 0;
  // The replication offset right after this client's last command that propagated something
  // (Redis's c->woff): what WAIT waits for.
  std::int64_t woff = 0;
  // The replica said REPLCONF capa psync2: +CONTINUE then names the replication id.
  bool repl_capa_psync2 = false;
  // A replica's last REPLCONF ACK (set when it starts streaming, as Redis's repl_ack_time), and
  // what it said with REPLCONF listening-port and ip-address: the shutdown's "Lagging replica"
  // log line names it by them (Redis's replicationGetSlaveName()).
  std::int64_t repl_ack_time_ms = 0;
  int repl_listening_port = 0;
  std::string repl_ip_address;
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
  // ── CLIENT REPLY OFF/SKIP ── suppress normal command replies (never pub/sub confirmations).
  bool reply_off = false;        // CLIENT REPLY OFF until CLIENT REPLY ON
  bool reply_skip = false;       // suppress THIS command's reply (CLIENT REPLY SKIP, armed)
  bool reply_skip_next = false;  // CLIENT REPLY SKIP just ran; arm skip for the next command
  // ── CLIENT NO-TOUCH ── when on, this client's commands don't bump keys' LRU/LFU access
  // time (except an explicit TOUCH), so OBJECT IDLETIME/FREQ stay stable for observation.
  bool no_touch = false;
  // ── Introspection (CLIENT LIST / MONITOR / CLIENT KILL) ──
  std::string addr;  // peer "ip:port", set at accept
  // A unix-socket or loopback (127.x, ::1) client: Redis's connIsLocal(), which decides what a
  // "local" value of enable-protected-configs allows. Set at accept.
  bool is_local = false;
  std::int64_t created_ms = 0;           // connection creation time
  std::int64_t last_interaction_ms = 0;  // last command time (for idle)
  std::string last_cmd;                  // last command name (lowercased)
  bool is_monitor = false;               // client issued MONITOR
  // Set by the event loop before each command: earlier replies to this client are not sent yet
  // (Redis's clientHasPendingReplies(), which SCRIPT DEBUG checks).
  bool has_pending_replies = false;
  // ── CLIENT PAUSE ── handle_command sets `postponed` instead of running a command the pause
  // holds (Redis's blockPostponeClient()); the event loop then keeps the command and everything
  // after it in the input buffer and runs them again, in `postpone_seq` order, when the pause ends.
  bool postponed = false;
  std::uint64_t postpone_seq = 0;
  // ── SHUTDOWN waiting for replicas (Redis's BLOCKED_SHUTDOWN) ── set by the SHUTDOWN handler
  // when the shutdown waits for lagging replicas; the event loop then runs nothing more from this
  // client until the server exits (no reply) or the shutdown fails or is aborted (the client gets
  // "Errors trying to SHUTDOWN. Check logs.", g_reply_blocked_on_shutdown_cb).
  bool blocked_on_shutdown = false;
};

std::string handle_command(const std::vector<std::string>& args, SessionState& session,
                           bool& should_close);
// Redis's command flags (kRedisCmd* in command_metadata.generated.hpp) of the command or
// subcommand that `args` runs (also under a name given by rename-command), from the pinned Redis
// command JSON; 0 for a name that runs no command.
std::uint64_t redis_command_flags(const std::vector<std::string>& args);
// CLIENT PAUSE and the pause of a shutdown that waits for replicas, for the event loop: checks
// whether a pause has timed out and returns true when the clients it held must run again
// (Redis's updatePausedActions()/unblockPostponedClients()).
bool client_pause_update();
// When the first current pause ends (0 when nothing is paused), so the event loop wakes up then.
std::int64_t client_pause_end_ms();
std::size_t command_count();
std::vector<std::string> command_names();
// dbfilename and appendfilename are relative to the working directory, which the config loader
// changes to `dir` the way Redis does. With appendonly on it opens the AOF, and returns Redis's
// error text when it cannot (startup must stop then); otherwise it returns "".
std::string configure_persistence(const std::string& dbfilename, bool appendonly,
                                  const std::string& appendfilename,
                                  const std::string& appendfsync);
bool load_aof_file(const std::string& path, std::string& err);
// Each event-loop pass: writes again what a failed AOF write left, and runs the everysec fsync.
void aof_cron(std::int64_t now_ms);
void reap_background_children();
void configure_runtime_port(int port);
// Set the configured role before loading persistence; connect once the event loop is ready.
void configure_replication(bool replica, const std::string& host, int port);
void start_configured_replication();
void configure_max_clients(int max_clients);
int configured_max_clients();  // read live by the accept loop, so CONFIG SET maxclients applies
// Raises the open-files soft limit so `max_clients` clients fit (Redis's adjustOpenFilesLimit())
// and returns how many clients the limit allows, which may be fewer. Defined in server.cpp.
int adjust_open_files_limit(int max_clients);
std::int64_t proto_max_bulk_len();         // proto-max-bulk-len, for the RESP parser
std::int64_t client_query_buffer_limit();  // client-query-buffer-limit
int tcp_keepalive_seconds();               // tcp-keepalive (0 = off), for new connections
int replication_timeout_seconds();         // repl-timeout, also bounds replica ACK silence
// ── Cluster configuration (wired from main.cpp at startup; startup-only, like Redis's
// IMMUTABLE_CONFIG for cluster-enabled) ──
void configure_cluster(bool enabled, const std::string& bind, int port,
                       const std::string& announce_ip, const std::string& announce_port,
                       const std::string& announce_bus_port);
// What this node tells other nodes and clients to reach it on. `announce_ip` empty falls back
// to the bound address — a documented simplification of Redis's live-connection auto-detection
// (see docs/decisions/p5-cluster-addressing-and-version-provenance.md); `announce_port`/`-bus-port`
// fall back to the client port and announced-port+10000, matching Redis (config.c:3105,3171-3172).
std::string cluster_announced_host();
int cluster_announced_port();
int cluster_announced_bus_port();
// A wildcard (any-interface) address that no specific peer could dial back — shared by the
// main.cpp startup refusal and CONFIG SET's cluster-announce-ip validation so the two checks
// cannot drift apart.
bool is_wildcard_announce_host(const std::string& host);
// ── Security configuration (wired from main.cpp at startup) ──
void configure_security(const std::string& requirepass, const std::string& masterauth,
                        const std::string& masteruser, bool protected_mode,
                        const std::string& enable_module_command,
                        const std::vector<RenameCommandDirective>& rename_commands);
// Builds the command table and applies the rename-command directives (already checked by the
// config loader against Redis's command table). Returns a config error report if a rename
// still cannot be applied ("" otherwise), and adds a warning for each line that renames a Redis
// command PeaDB does not implement. Call once at startup, after configure_security() and before
// anything else uses the command table.
std::string validate_command_renames(std::vector<std::string>& warnings);
// Sets the log level from a Redis loglevel name (debug, verbose, notice, warning, nothing),
// as reported by CONFIG GET loglevel.
void configure_log_level(const std::string& name);
bool protected_mode_enabled();  // read by the server accept loop
bool requirepass_is_set();      // read by the server accept loop (protected-mode bypass)
void configure_maxmemory(const std::string& spec);          // "100mb"/bytes; "" = unlimited
void configure_maxmemory_policy(const std::string& spec);   // e.g. "allkeys-lru"; "" = noeviction
void configure_maxmemory_samples(const std::string& spec);  // eviction sample size; "" = 5
void init_replication_id();           // randomize the replication id at startup (call once in main)
void functions_register_rdb_hooks();  // wire RDB save/load to the function-library registry (call
                                      // once in main)
void configure_timeout(const std::string& spec);  // idle-client timeout (seconds); "" = keep 0
void configure_tcp_backlog(int backlog);          // listen(2) backlog, for CONFIG GET reporting
// Apply *-max-listpack-*/set-max-intset-entries thresholds from the checked config settings
// (ServerConfig::raw, canonical names) — the upstream harness passes per-test overrides this way.
void configure_encoding_limits(const std::unordered_map<std::string, std::string>& raw);
// Apply the config-file settings that CONFIG SET also changes at runtime and that have no
// ServerConfig field: min-replicas-to-write, replica-serve-stale-data, busy-reply-threshold,
// lazyfree-lazy-server-del, stop-writes-on-bgsave-error, aof-load-truncated,
// slowlog-log-slower-than, slowlog-max-len, latency-monitor-threshold, proto-max-bulk-len,
// client-query-buffer-limit, tcp-keepalive, lfu-log-factor, rdbchecksum, rdb-key-save-delay,
// shutdown-on-sigterm and shutdown-on-sigint (values already checked). It also keeps every other
// setting the config named, for CONFIG GET; `config_file` is the config file PeaDB was started with
// ("" for none), for CONFIG REWRITE.
void configure_runtime_settings(const std::unordered_map<std::string, std::string>& raw,
                                const std::string& config_file);
void script_set_repl_flags(int flags);  // redis.set_repl() from the Lua engine (1=AOF, 2=replicas)
int client_timeout_seconds();           // current idle timeout (0 = disabled)
bool client_has_subscriptions(int fd);  // exempt pub/sub clients from idle reaping

// client-output-buffer-limit: per-class output buffer caps. hard=0 / soft=0 means no limit.
enum CoblClass { kCoblNormal = 0, kCoblReplica = 1, kCoblPubsub = 2 };
struct CoblLimit {
  std::int64_t hard;
  std::int64_t soft;
  int soft_seconds;
};
void configure_client_output_buffer_limit(
    const std::string& spec);                               // "normal H S Sec slave ... pubsub ..."
CoblLimit cobl_for(int cls);                                // limits for a CoblClass
std::string client_output_buffer_limit_str();               // current config as a CONFIG GET string
void configure_save(const std::string& spec);               // "sec changes [sec changes ...]"
void configure_repl_backlog_size(const std::string& spec);  // e.g. "256mb"
void configure_notify_keyspace_events(const std::string& spec);  // e.g. "KEA"
// Makes a master propagate the DEL and send the `expired` event of each key DataStore deletes
// because its TTL has passed (compat/delta.md item 37). Called once, before any data loads.
void configure_lazy_expire();
void background_save_cron();     // event-loop save-point check (triggers BGSAVE)
void run_active_expire_cycle();  // event-loop active expiry (master propagates DEL; replica no-op)
void run_eviction_cycle();       // every event-loop pass: used_memory peak, and the eviction timer
bool eviction_proc_running();    // the eviction timer is on: the event loop must not sleep
void databases_cron();           // event-loop shrink of sparse keyspace tables (no fork child)
void set_active_expire_enabled(bool enabled);
bool active_expire_enabled();
std::size_t replication_event_count();
// The replication stream from offset `from` (counted from 0) to its end, for a replica. False when
// the backlog no longer holds `from` (the replica cannot be served and must sync again).
bool replication_stream_from(std::size_t from, std::string& out);
// server.cpp has given every replica the whole replication backlog (backlog_append() needs to hand
// out bytes at once only once more than the backlog size has been written since).
void replicas_took_backlog();
// Start a disk-backed replication BGSAVE: 1 started, 0 another child is running, -1 failed.
// The event loop batches waiting replicas onto this snapshot's id/offset before more writes.
struct RdbReplicationInfo;
int start_replication_snapshot(RdbReplicationInfo& info);
extern std::function<void(const std::string& path, bool success)> g_replication_snapshot_done_cb;
// beforeSleep()'s GETACK: WAIT asks the replicas for an ACK once per event-loop pass, after the
// pass's commands (outside the WAIT, so it is not the client's write). replica_getack_pending()
// says one waits (the event loop must not sleep then); send_pending_getack() adds it to the stream.
bool replica_getack_pending();
void send_pending_getack();
// Copies into every replica's output buffer what it has not taken from the replication backlog
// yet and then, when `data` is given, those bytes, which are about to be appended; called before
// the backlog would overwrite bytes a replica still needs (compat/delta.md item 40). Wired by
// server.cpp.
extern std::function<void(const std::string* data)> g_feed_replicas_cb;
// A pipe the replica's link thread writes a byte to when it has queued commands from its master,
// so the event loop applies them at once instead of at its next 100 ms tick (server.cpp polls the
// read end and calls take_replica_wakeup() when it is readable).
void set_replica_wakeup_fd(int write_fd, int read_fd);
void take_replica_wakeup();
// Each event-loop pass, Redis's replicationCron() part: a replica tells its master every second
// how much of its stream it has applied (REPLCONF ACK).
void replication_cron(std::int64_t now_ms);
void replica_disconnected();  // call after removing a replica session
// Event-loop cron: closes the MIGRATE connections idle for more than 10 seconds (once a second).
void migrate_cron(std::int64_t now_ms);
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
// A command a script called was refused before it ran (its rejected_calls and an ERR errorstat).
void record_lua_script_rejected(const std::string& cmd);
// scriptVerifyAllowStale(): on a stale replica (link down, replica-serve-stale-data no) a script
// may call only the commands Redis flags STALE.
bool script_command_refused_as_stale(const std::vector<std::string>& args);

// ── Shutdown ──
// request_shutdown() makes the event loop stop; the SHUTDOWN command and shutdown_cron() call it
// once prepare_for_shutdown() or the end of the wait for replicas has succeeded.
void request_shutdown();
bool shutdown_requested();
// Redis's SHUTDOWN_* flags (server.h).
inline constexpr int kShutdownSave = 1;
inline constexpr int kShutdownNosave = 2;
inline constexpr int kShutdownNow = 4;
inline constexpr int kShutdownForce = 8;
// Redis's prepareForShutdown() (server.c), shared by the SHUTDOWN command and SIGTERM/SIGINT.
// Without NOW, with shutdown-timeout not 0 and a replica whose ACK is behind the replication
// offset, it starts the wait for replicas (shutdown_initiated(): writes are paused, the replicas
// are asked for an ACK, shutdown_cron() finishes it) and returns false. Otherwise it runs
// finishShutdown(): logs the lagging replicas, stops a BGSAVE or AOF rewrite child, fsyncs the
// AOF, does the final RDB save (save points without NOSAVE, or SAVE) and logs each step. Returns
// true when the server may exit now; on a failure it logs why, cancels the shutdown (a pending
// signal is forgotten, the blocked SHUTDOWN clients get the error) and returns false (unless
// FORCE). While a shutdown waits it returns false at once.
bool prepare_for_shutdown(int flags);
// Redis's isShutdownInitiated(): a shutdown waits for lagging replicas.
bool shutdown_initiated();
// SHUTDOWN ABORT (Redis's abortShutdown()): cancels a shutdown that waits for replicas, or forgets
// a signal the event loop has not handled yet; false when there is neither.
bool abort_shutdown();
// The SIGTERM/SIGINT handler (Redis's sigShutdownHandler()): logs and asks for a shutdown, which
// shutdown_cron() runs from the event loop with the shutdown-on-sigterm/-sigint flags. A second
// SIGINT exits at once with code 1 and deletes the final save's temp-<pid>.rdb.
void shutdown_signal_handler(int sig);
// The shutdown part of Redis's serverCron(), run on every event-loop pass: starts the shutdown a
// signal asked for, and finishes a shutdown that waits once every replica has acknowledged the
// whole replication stream or shutdown-timeout has passed.
void shutdown_cron();
bool shutdown_signal_pending();
// Redis's disconnectAllBlockedClients(): answers every client blocked on keys or in WAIT with
// `error` (a RESP error line), when REPLICAOF makes this server a replica. Wired by server.cpp.
extern std::function<void(const std::string& error)> g_unblock_blocked_clients_cb;
// Calls fn for each replica's session (server.cpp walks its replica connections).
extern std::function<void(const std::function<void(const SessionState&)>&)> g_for_each_replica_cb;
// Redis's replyToClientsBlockedOnShutdown(): server.cpp unblocks every client blocked on SHUTDOWN,
// sends it what reply(client) returns ("Errors trying to SHUTDOWN. Check logs.") and then runs
// its next commands.
extern std::function<void(const std::function<std::string(const SessionState&)>& reply)>
    g_reply_blocked_on_shutdown_cb;
// Set around the startup load: a signal then shuts down without saving (Redis's server.loading).
void set_loading(bool loading);
// shutdown-on-sigterm/-sigint values: "save nosave now force" words <-> kShutdown* flags.
int parse_shutdown_flags(const std::string& value);
std::string shutdown_flags_to_str(int flags);

// ── Server stats (set from server.cpp, read from command.cpp INFO handler) ──
extern std::atomic<int> g_connected_clients;
extern std::atomic<std::int64_t> g_total_connections_received;
extern std::atomic<std::int64_t> g_total_commands_processed;
extern std::atomic<std::int64_t> g_server_start_time;
// Callback: given a target offset, returns how many replicas have acked >= offset
extern std::function<int(std::int64_t)> g_count_synced_replicas;
// Callback: returns the number of currently-connected replica sessions (replica_stream=true).
extern std::function<int()> g_count_connected_replicas;
// Bytes held by the replicas' output buffers (server.cpp): not counted against maxmemory.
extern std::function<std::size_t()> g_replica_output_buffers_cb;
// Bytes held by the query and output buffers of (normal clients, replicas): INFO's
// mem_clients_normal and mem_clients_slaves.
extern std::function<std::pair<std::size_t, std::size_t>()> g_client_buffers_cb;
// Callback: returns the number of currently-blocked clients (BLPOP/BRPOP/etc.).
extern std::function<int()> g_blocked_clients_cb;
// CLIENT KILL's filters (Redis's clientKillCommand()): a client matches when it passes every
// filter that is set.
enum class ClientType { Normal, Slave, Pubsub, Master };
struct ClientKillFilter {
  std::string addr;                // ADDR ip:port (the peer), or the old form's argument
  std::string laddr;               // LADDR ip:port (the local end of the client's socket)
  std::uint64_t id = 0;            // ID; 0 = any
  std::optional<ClientType> type;  // TYPE
  bool default_user = false;       // USER default (the upstream master has no ACL user)
  bool skipme = true;              // SKIPME yes: the calling client is never killed
  int self_fd = -1;                // the calling client
};
// The upstream link is owned by the replication thread, outside the event-loop session map.
int kill_master_link(const ClientKillFilter& filter);
std::string master_client_list();
// Callback: closes the clients that match, except the caller, which is only reported in
// kill_self (it is closed after its reply). Returns the number matched, the caller included.
// Wired by server.cpp (owns fd lifecycle).
extern std::function<int(const ClientKillFilter& filter, bool& kill_self)> g_client_kill_cb;
// CLIENT UNBLOCK <id> [TIMEOUT|ERROR]: unblock a blocked client by id. error=true
// delivers -UNBLOCKED; error=false delivers the command's timeout reply. Returns 1 if
// a matching, currently-blocked client was found, else 0.
extern std::function<int(std::uint64_t client_id, bool error)> g_client_unblock_cb;
// Callback: closes the listening sockets and every client connection. Called only in a BGSAVE or
// AOF rewrite child right after fork(), so the port is free and clients see their connection close
// when the server dies while a child runs (Redis's closeChildUnusedResourceAfterFork() closes the
// listeners; Redis keeps the client sockets and shuts them down when it closes one). Wired by
// server.cpp (owns the fds).
extern std::function<void()> g_close_sockets_in_child_cb;
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

}  // namespace peadb
