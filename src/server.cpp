#include "server.hpp"

#include "command.hpp"
#include "datastore.hpp"
#include "logger.hpp"
#include "poller.hpp"
#include "protocol.hpp"
#include "server_context.hpp"

#include <arpa/inet.h>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <exception>
#include <string>
#include <string_view>
#include <limits>
#include <unordered_map>
#include <vector>

namespace peadb {

namespace {

bool set_nonblocking(int fd) {
  const int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return false;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

void set_cloexec(int fd) {
  const int flags = fcntl(fd, F_GETFD, 0);
  if (flags >= 0) fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
}

// Top-level exception boundary: a single bad command (bad_alloc, length_error, …)
// must degrade to a RESP error for that client, never terminate the single event
// loop and take down every connection.
std::string safe_handle_command(const std::vector<std::string>& args,
                                SessionState& session, bool& should_close) {
  std::string reply;
  try {
    reply = handle_command(args, session, should_close);
  } catch (const std::exception& ex) {
    log(LogLevel::Error, std::string("command handler exception: ") + ex.what());
    reply = "-ERR internal error\r\n";
  } catch (...) {
    log(LogLevel::Error, "command handler exception (unknown)");
    reply = "-ERR internal error\r\n";
  }
  // CLIENT REPLY OFF/SKIP: suppress normal command replies. Pub/sub (un)subscribe
  // confirmations are push-like and must always reach the client, so they bypass this.
  std::string cmd0;
  if (!args.empty()) { cmd0 = args[0]; for (auto& c : cmd0) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c))); }
  const bool subscribe_family = cmd0 == "SUBSCRIBE" || cmd0 == "UNSUBSCRIBE" ||
                                cmd0 == "PSUBSCRIBE" || cmd0 == "PUNSUBSCRIBE" ||
                                cmd0 == "SSUBSCRIBE" || cmd0 == "SUNSUBSCRIBE";
  const bool suppress = (session.reply_off || session.reply_skip) && !subscribe_family;
  // Advance the skip state: consume the armed skip for this command, then arm it for the
  // next command if a CLIENT REPLY SKIP just ran.
  session.reply_skip = false;
  if (session.reply_skip_next) { session.reply_skip = true; session.reply_skip_next = false; }
  if (suppress) return std::string();
  return reply;
}

bool set_tcp_nodelay(int fd) {
  int yes = 1;
  return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == 0;
}

// Enable TCP keepalive so half-open connections (peer vanished without FIN) are detected
// and reaped by the kernel instead of lingering forever against maxclients.
void set_tcp_keepalive(int fd) {
  int yes = 1;
  setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes));
#ifdef __linux__
  int idle = 300, intvl = 30, cnt = 3;  // first probe after 5 min, then every 30s, 3 times
  setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
  setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
  setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));
#endif
}

int accept_nonblock(int listen_fd, sockaddr* addr, socklen_t* addrlen) {
#ifdef __linux__
  return accept4(listen_fd, addr, addrlen, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
  int fd = ::accept(listen_fd, addr, addrlen);
  if (fd >= 0) { set_nonblocking(fd); set_cloexec(fd); }
  return fd;
#endif
}

// Offset-tracked write buffer to avoid O(n) erase on partial writes
struct WriteBuf {
  std::string data;
  std::size_t offset = 0;
  std::int64_t soft_since_ms = 0;  // when this buffer first exceeded its class soft limit (0 = not over)

  bool empty() const { return offset >= data.size(); }
  std::size_t remaining() const { return data.size() - offset; }
  const char* ptr() const { return data.data() + offset; }
  void append(const std::string& s) { data.append(s); }
  void advance(std::size_t n) { offset += n; }
  void compact() {
    // Only compact when we've consumed more than half the buffer to avoid O(n) erase on every tick
    if (offset > data.size() / 2) {
      data.erase(0, offset);
      offset = 0;
    }
  }
};

} // namespace

int run_server(const ServerConfig& config) {
  configure_max_clients(config.maxclients);
  int max_clients = configured_max_clients();
  constexpr unsigned long long kFdReserve = 64;

  {
    auto rlim_to_ull = [](rlim_t value) -> unsigned long long {
      if (value == RLIM_INFINITY) return std::numeric_limits<unsigned long long>::max();
      return static_cast<unsigned long long>(value);
    };

    rlimit nofile {};
    if (::getrlimit(RLIMIT_NOFILE, &nofile) == 0) {
      const unsigned long long soft_before = rlim_to_ull(nofile.rlim_cur);
      const unsigned long long hard_before = rlim_to_ull(nofile.rlim_max);
      const unsigned long long desired = static_cast<unsigned long long>(max_clients) + kFdReserve;

      if (soft_before < desired && hard_before >= desired) {
        rlimit raised = nofile;
        raised.rlim_cur = static_cast<rlim_t>(desired);
        if (::setrlimit(RLIMIT_NOFILE, &raised) == 0) {
          if (::getrlimit(RLIMIT_NOFILE, &nofile) != 0) {
            nofile = raised;
          }
          log(LogLevel::Info,
              "raised nofile soft limit to " +
                  std::to_string(static_cast<unsigned long long>(nofile.rlim_cur)) +
                  " for maxclients compatibility");
        } else {
          log(LogLevel::Warn,
              "failed to raise nofile soft limit: " + std::string(std::strerror(errno)));
        }
      }

      const unsigned long long soft_now = rlim_to_ull(nofile.rlim_cur);
      unsigned long long capacity_ull = 1;
      if (soft_now > kFdReserve) {
        capacity_ull = soft_now - kFdReserve;
      }
      const unsigned long long int_max_ull =
          static_cast<unsigned long long>(std::numeric_limits<int>::max());
      const int capacity = static_cast<int>((capacity_ull > int_max_ull) ? int_max_ull : capacity_ull);

      if (capacity < max_clients) {
        configure_max_clients(capacity);
        max_clients = configured_max_clients();
        log(LogLevel::Warn,
            "lowering effective maxclients to " + std::to_string(max_clients) +
                " due to RLIMIT_NOFILE; increase ulimit -n to allow more clients");
      }

      log(LogLevel::Info,
          "process nofile limit soft=" + std::to_string(rlim_to_ull(nofile.rlim_cur)) +
              ", hard=" + std::to_string(rlim_to_ull(nofile.rlim_max)) +
              ", effective maxclients=" + std::to_string(max_clients));
    } else {
      log(LogLevel::Warn, "getrlimit(RLIMIT_NOFILE) failed: " + std::string(std::strerror(errno)));
    }
  }

  const int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    log(LogLevel::Error, "socket() failed: " + std::string(std::strerror(errno)));
    return 1;
  }

  int one = 1;
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  set_nonblocking(listen_fd);

  sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(config.port));
  if (inet_pton(AF_INET, config.bind.c_str(), &addr.sin_addr) != 1) {
    log(LogLevel::Error, "invalid bind address: " + config.bind);
    close(listen_fd);
    return 1;
  }

  if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    // "Failed listening on port" is grepped by the upstream Redis test harness to
    // detect port-in-use and retry with a different port — keep the phrase intact.
    log(LogLevel::Error, "bind() failed: " + std::string(std::strerror(errno)) +
                             " — Failed listening on port " + std::to_string(config.port) + " (tcp)");
    close(listen_fd);
    return 1;
  }

  if (listen(listen_fd, config.tcp_backlog) != 0) {
    log(LogLevel::Error, "listen() failed: " + std::string(std::strerror(errno)) +
                             " — Failed listening on port " + std::to_string(config.port) + " (tcp)");
    close(listen_fd);
    return 1;
  }

  // Optional AF_UNIX listener (redis.conf `unixsocket`). The upstream test harness
  // force-adds `unixsocket <dir>/socket` to every spawned server's config.
  int unix_fd = -1;
  if (!config.unixsocket.empty()) {
    unix_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    sockaddr_un uaddr {};
    uaddr.sun_family = AF_UNIX;
    if (unix_fd < 0 || config.unixsocket.size() >= sizeof(uaddr.sun_path)) {
      log(LogLevel::Error, "unixsocket setup failed (path too long or socket() error): " + config.unixsocket);
      if (unix_fd >= 0) close(unix_fd);
      close(listen_fd);
      return 1;
    }
    std::strncpy(uaddr.sun_path, config.unixsocket.c_str(), sizeof(uaddr.sun_path) - 1);
    ::unlink(config.unixsocket.c_str());  // remove a stale socket file from a previous run
    set_nonblocking(unix_fd);
    if (bind(unix_fd, reinterpret_cast<sockaddr*>(&uaddr), sizeof(uaddr)) != 0 ||
        listen(unix_fd, config.tcp_backlog) != 0) {
      log(LogLevel::Error, "Failed listening on unix socket " + config.unixsocket + ": " +
                               std::string(std::strerror(errno)));
      close(unix_fd);
      close(listen_fd);
      return 1;
    }
  }

  int reserved_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
  if (reserved_fd < 0) {
    log(LogLevel::Warn, "failed to open reserved fd for EMFILE handling: " + std::string(std::strerror(errno)));
  }

  log(LogLevel::Info, "peadb-server listening on " + config.bind + ":" + std::to_string(config.port));

  // Loud warning when reachable from the network without authentication. protected-mode
  // (on by default) still refuses non-loopback clients in this state, but operators who
  // disable it must know they are exposing an unauthenticated server.
  if (!requirepass_is_set() && config.bind != "127.0.0.1" && config.bind != "::1") {
    if (protected_mode_enabled()) {
      log(LogLevel::Warn, "no requirepass set and bound to " + config.bind +
          " — protected-mode is ON, so only loopback clients are accepted. Set 'requirepass' "
          "to allow remote clients, or 'protected-mode no' to accept them unauthenticated.");
    } else {
      log(LogLevel::Warn, "SECURITY: protected-mode is OFF, no requirepass set, and bound to " +
          config.bind + " — this server accepts UNAUTHENTICATED connections from the network.");
    }
  }

  // ── Cluster bus listener on port N+10000 ──
  const int cluster_bus_port = config.port + 10000;
  int cbus_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (cbus_fd >= 0) {
    int one_c = 1;
    setsockopt(cbus_fd, SOL_SOCKET, SO_REUSEADDR, &one_c, sizeof(one_c));
    set_nonblocking(cbus_fd);
    sockaddr_in cbus_addr {};
    cbus_addr.sin_family = AF_INET;
    cbus_addr.sin_port = htons(static_cast<uint16_t>(cluster_bus_port));
    inet_pton(AF_INET, config.bind.c_str(), &cbus_addr.sin_addr);
    if (bind(cbus_fd, reinterpret_cast<sockaddr*>(&cbus_addr), sizeof(cbus_addr)) == 0 &&
        listen(cbus_fd, 16) == 0) {
      log(LogLevel::Info, "cluster bus listening on port " + std::to_string(cluster_bus_port));
    } else {
      log(LogLevel::Warn, "cluster bus bind/listen failed on port " + std::to_string(cluster_bus_port));
      close(cbus_fd);
      cbus_fd = -1;
    }
  }
  // Set of cluster bus client fds (gossip connections)
  std::unordered_map<int, std::string> cbus_buffers;

  Poller poller;
  if (!poller.create()) {
    log(LogLevel::Error, "poller create failed: " + std::string(std::strerror(errno)));
    close(listen_fd);
    if (cbus_fd >= 0) close(cbus_fd);
    return 1;
  }
  poller.add_read(listen_fd);
  if (unix_fd >= 0) poller.add_read(unix_fd);
  if (cbus_fd >= 0) poller.add_read(cbus_fd);
  // Third line of the startup log contract; the upstream harness counts these lines
  // to detect (re)starts.
  log(LogLevel::Info, "Ready to accept connections tcp");
  std::vector<PollEvent> ep_events;
  std::unordered_map<int, std::string> buffers;
  std::unordered_map<int, WriteBuf> write_buffers;
  std::unordered_map<int, SessionState> sessions;
  std::int64_t last_active_expire_ms = DataStore::now_ms();
  std::int64_t last_idle_reap_ms = DataStore::now_ms();
  int blocked_count = 0;
  int wait_count = 0;
  int replica_count = 0;

  // Wire up the WAIT callback so it can count synced replicas
  g_count_synced_replicas = [&](std::int64_t target_offset) -> int {
    int count = 0;
    for (auto& [fd, sess] : sessions) {
      if (sess.replica_stream && sess.repl_ack_offset >= target_offset) ++count;
    }
    return count;
  };

  // Wire up the INFO replication callback so connected_slaves reflects live sessions.
  g_count_connected_replicas = [&]() -> int {
    int count = 0;
    for (auto& [fd, sess] : sessions) {
      if (sess.replica_stream) ++count;
    }
    return count;
  };

  g_blocked_clients_cb = [&]() -> int { return blocked_count; };

  // Wire up the CLIENT LIST callback
  g_client_list_callback = [&]() -> std::string {
    std::string out;
    const auto now = DataStore::now_ms();
    for (auto& [fd, sess] : sessions) {
      const std::int64_t age = sess.created_ms ? (now - sess.created_ms) / 1000 : 0;
      const std::int64_t idle = sess.last_interaction_ms ? (now - sess.last_interaction_ms) / 1000 : 0;
      auto qit = buffers.find(fd);
      const std::size_t qbuf = (qit != buffers.end()) ? qit->second.size() : 0;
      out += "id=" + std::to_string(sess.client_id);
      out += " addr=" + sess.addr;
      out += " fd=" + std::to_string(fd);
      out += " name=" + sess.client_name;
      out += " lib-name=" + sess.lib_name;
      out += " lib-ver=" + sess.lib_ver;
      out += " db=" + std::to_string(sess.db_index);
      out += " sub=0 psub=0 ssub=0";
      out += " multi=" + std::string(sess.in_multi ? std::to_string(static_cast<int>(sess.queued.size())) : "-1");
      out += " watch=" + std::to_string(sess.watched_digests.size());
      out += " qbuf=" + std::to_string(qbuf) + " qbuf-free=0 argv-mem=0 multi-mem=0 tot-mem=" + std::to_string(qbuf + 16384);
      out += " net-i=0 net-o=0";
      out += " age=" + std::to_string(age) + " idle=" + std::to_string(idle);
      std::string flags;
      if (sess.replica_stream) flags += "S";
      if (sess.is_monitor) flags += "O";
      if (sess.blocked.type != BlockType::None) flags += "b";
      if (flags.empty()) flags = "N";
      out += " flags=" + flags;
      out += " cmd=" + (sess.last_cmd.empty() ? std::string("NULL") : sess.last_cmd);
      out += " user=" + sess.user + " redir=-1";
      out += " resp=" + std::to_string(static_cast<int>(sess.resp_version));
      out += "\n";
    }
    return out;
  };

  // Mirror callbacks into the ServerContext struct for future callers.
  server_ctx().callbacks.count_synced_replicas = g_count_synced_replicas;
  server_ctx().callbacks.client_list = g_client_list_callback;

  // Per-class client-output-buffer-limit (normal/replica/pubsub). A slow consumer or a
  // lagging replica can no longer buffer unbounded and OOM the single process. Returning
  // false from enqueue_output makes the caller close the offending client.
  auto cobl_class_of = [&](int fd) -> int {
    auto it = sessions.find(fd);
    if (it != sessions.end() && it->second.replica_stream) return kCoblReplica;
    if (client_has_subscriptions(fd)) return kCoblPubsub;
    return kCoblNormal;
  };

  // Clients whose output buffer overflowed its class limit during an async send (pub/sub)
  // are recorded here and closed by the event loop, since those senders ignore the return.
  std::vector<int> obuf_overflow_fds;

  auto enqueue_output = [&](int fd, const std::string& data) -> bool {
    if (data.empty()) return true;
    auto& wbuf = write_buffers[fd];
    const bool was_empty = wbuf.empty();
    wbuf.append(data);
    while (!wbuf.empty()) {
      const ssize_t n = ::write(fd, wbuf.ptr(), wbuf.remaining());
      if (n < 0) {
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        return false;
      }
      if (n == 0) break;
      wbuf.advance(static_cast<std::size_t>(n));
    }
    // Enforce the class output-buffer limit on what remains BUFFERED after the write
    // attempt (Redis semantics — a large message the socket fully absorbs is fine; a
    // persistent backlog from a slow consumer / lagging replica trips it). Async senders
    // (pub/sub) ignore the return value, so record the fd for the loop to close.
    const std::size_t remaining = wbuf.remaining();
    const CoblLimit lim = cobl_for(cobl_class_of(fd));
    bool overflow = false;
    if (lim.hard > 0 && remaining > static_cast<std::size_t>(lim.hard)) {
      overflow = true;
    } else if (lim.soft > 0 && lim.soft_seconds > 0 && remaining > static_cast<std::size_t>(lim.soft)) {
      const std::int64_t now = DataStore::now_ms();
      if (wbuf.soft_since_ms == 0) wbuf.soft_since_ms = now;
      else if (now - wbuf.soft_since_ms >= static_cast<std::int64_t>(lim.soft_seconds) * 1000) overflow = true;
    } else {
      wbuf.soft_since_ms = 0;  // back under the soft limit — reset the timer
    }
    if (overflow) { obuf_overflow_fds.push_back(fd); return false; }
    if (wbuf.empty()) {
      wbuf.data.clear();
      wbuf.offset = 0;
    } else {
      wbuf.compact();
    }
    // Register write interest if data remains in the write buffer.
    if (!wbuf.empty() && was_empty) {
      poller.set_readwrite(fd);
    }
    return true;
  };

  // Helper: flush any pending output for a client fd.
  auto flush_output = [&](int fd) -> bool {
    auto it = write_buffers.find(fd);
    if (it == write_buffers.end() || it->second.empty()) return true;
    auto& wbuf = it->second;
    while (!wbuf.empty()) {
      const ssize_t n = ::write(fd, wbuf.ptr(), wbuf.remaining());
      if (n < 0) {
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        return false;
      }
      if (n == 0) break;
      wbuf.advance(static_cast<std::size_t>(n));
    }
    if (wbuf.empty()) {
      wbuf.data.clear();
      wbuf.offset = 0;
    } else if (wbuf.offset > 65536) {
      wbuf.compact();
    }
    // Remove write interest once the write buffer is fully drained.
    if (wbuf.empty()) {
      poller.set_read_only(fd);
    }
    return true;
  };

  // Self-published messages (a client PUBLISHing/keyspace-notifying a channel it is itself
  // subscribed to) must be delivered AFTER that command's own reply — including after the
  // whole EXEC array or script reply. Redis buffers them onto the executing client's reply;
  // we do the same by capturing frames destined for the currently-dispatching fd here and
  // appending them to the reply once the top-level command returns (see the dispatch loop).
  int self_dispatch_fd = -1;
  std::string self_pubsub_pending;

  // Wire up the pub/sub send callback (must be after enqueue_output is defined)
  g_pubsub_send = [&](int fd, const std::string& data) -> bool {
    if (fd == self_dispatch_fd) { self_pubsub_pending += data; return true; }
    if (!sessions.count(fd)) return false;
    return enqueue_output(fd, data);
  };

  // Helper: close a client connection and clean up all associated state. Idempotent —
  // safe to call twice (e.g. a synchronous caller closed on a false enqueue_output and the
  // deferred output-buffer-overflow drain also targets the same fd).
  auto close_client = [&](int fd) {
    if (!sessions.count(fd) && !write_buffers.count(fd)) return;
    // A client that disconnects while blocked/waiting must release its slot, else the
    // blocked_clients counter leaks and later blocking commands (and INFO) miscount.
    auto scit = sessions.find(fd);
    if (scit != sessions.end()) {
      if (scit->second.blocked.type != BlockType::None && blocked_count > 0) --blocked_count;
      if (scit->second.wait_state.has_value() && wait_count > 0) --wait_count;
    }
    pubsub_unsubscribe_all(fd);
    on_client_disconnect(fd);
    poller.remove(fd);
    ::close(fd);
    buffers.erase(fd);
    write_buffers.erase(fd);
    sessions.erase(fd);
    g_connected_clients.fetch_sub(1);
  };

  // CLIENT KILL: close clients matching a filter, except skip_fd (the caller, which
  // closes itself via should_close). Safe to erase here — the event loop skips fds no
  // longer in `sessions` at the top of each ready-fd iteration.
  g_client_kill_cb = [&](const std::string& by, const std::string& val, int skip_fd) -> int {
    std::vector<int> to_kill;
    for (auto& [fd, sess] : sessions) {
      if (fd == skip_fd) continue;
      bool match = false;
      if (by == "ID") match = (std::to_string(sess.client_id) == val);
      else if (by == "ADDR") match = (sess.addr == val);
      else if (by == "ANY") match = true;
      if (match) to_kill.push_back(fd);
    }
    for (int fd : to_kill) close_client(fd);
    return static_cast<int>(to_kill.size());
  };


  // Drain any pipelined commands sitting in buffers[fd] after a BLPOP/WAIT
  // wakeup. Without this, those commands stall until the client sends more data.
  // Returns fds that should be closed (connection error or close-on-command).
  auto drain_pipelined = [&](const std::vector<int>& fds,
                              int& blocked_count_ref, int& wait_count_ref,
                              std::vector<int>& to_close_out) {
    for (int fd : fds) {
      if (!sessions.count(fd)) continue;
      auto& session = sessions[fd];
      auto& input = buffers[fd];
      if (input.empty()) continue;
      std::string batch;
      bool sc = false;
      std::size_t consumed = 0;
      while (true) {
        const std::string_view rem(input.data() + consumed, input.size() - consumed);
        auto parsed = parse_one_command(rem, requirepass_is_set() && !session.authenticated);
        if (!parsed.has_value()) break;
        g_total_commands_processed.fetch_add(1);
        self_dispatch_fd = session.fd;
        batch += safe_handle_command(parsed->args, session, sc);
        batch += self_pubsub_pending;  // self-directed messages come after the reply
        self_pubsub_pending.clear();
        self_dispatch_fd = -1;
        consumed += parsed->consumed;
        if (sc || session.blocked.type != BlockType::None || session.wait_state.has_value()) break;
      }
      if (consumed > 0) {
        if (consumed == input.size()) input.clear();
        else input.erase(0, consumed);
      }
      if (!batch.empty() && !enqueue_output(fd, batch)) to_close_out.push_back(fd);
      else if (sc) to_close_out.push_back(fd);
      if (session.blocked.type != BlockType::None) ++blocked_count_ref;
      if (session.wait_state.has_value()) ++wait_count_ref;
    }
  };

  g_client_unblock_cb = [&](std::uint64_t client_id, bool error) -> int {
    for (auto& [fd, sess] : sessions) {
      if (sess.client_id != client_id || sess.blocked.type == BlockType::None) continue;
      // TIMEOUT: deliver the command's normal timeout reply (nil array, or nil bulk for
      // the BLMOVE/BRPOPLPUSH move forms). ERROR: deliver -UNBLOCKED. Then clear the
      // block and drain any pipelined follow-up, exactly like a natural wakeup.
      const bool r3 = sess.resp_version == RespVersion::Resp3;
      std::string reply;
      if (error) {
        reply = "-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n";
      } else {
        std::string bcmd;
        if (!sess.blocked.original_args.empty()) {
          bcmd = sess.blocked.original_args[0];
          for (auto& c : bcmd) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
        const bool null_bulk = bcmd == "BLMOVE" || bcmd == "BRPOPLPUSH";
        reply = r3 ? "_\r\n" : (null_bulk ? "$-1\r\n" : "*-1\r\n");
      }
      sess.blocked = BlockedState{};  // clear the block
      --blocked_count;
      if (!enqueue_output(fd, reply)) { close_client(fd); return 1; }
      std::vector<int> one{fd}, dclose;
      drain_pipelined(one, blocked_count, wait_count, dclose);
      for (int c : dclose) close_client(c);
      return 1;
    }
    return 0;
  };

  // Recheck blocked clients (BLPOP/BRPOP/BLMOVE/BZPOPMIN/XREAD…): deliver to any whose
  // key now has data, or whose block timed out. Extracted so it can run both at loop top
  // and again right after processing readable fds — the latter removes the ~100ms latency
  // floor (a write and the unblock land in the same iteration instead of the next poll).
  auto recheck_blocked = [&]() {
    if (blocked_count <= 0) return;
    std::vector<int> to_close, to_drain;
    // Serve blocked clients FIFO (Redis serveClientsBlockedOnKeys order): the earliest
    // blocker gets the data. The sessions map iterates in arbitrary order, so collect
    // and sort by the block sequence number.
    std::vector<std::pair<std::uint64_t, int>> ordered;
    for (auto& [bfd, session] : sessions) {
      if (session.blocked.type != BlockType::None) ordered.push_back({session.blocked.seq, bfd});
    }
    std::sort(ordered.begin(), ordered.end());
    for (const auto& [seq, bfd] : ordered) {
      auto sit = sessions.find(bfd);
      if (sit == sessions.end() || sit->second.blocked.type == BlockType::None) continue;
      auto& session = sit->second;
      const std::string reply = try_unblock_client(session);
      if (!reply.empty()) {
        --blocked_count;
        if (!enqueue_output(bfd, reply)) to_close.push_back(bfd);
        else to_drain.push_back(bfd);
      }
    }
    for (int cfd : to_close) close_client(cfd);
    std::vector<int> drain_close;
    drain_pipelined(to_drain, blocked_count, wait_count, drain_close);
    for (int cfd : drain_close) close_client(cfd);
  };

  auto recheck_wait = [&]() {
    if (wait_count <= 0) return;
    const auto wloop_now = DataStore::now_ms();
    std::vector<int> to_close, to_drain;
    for (auto& [wfd, session] : sessions) {
      if (!session.wait_state.has_value()) continue;
      auto& ws = session.wait_state.value();
      int synced = g_count_synced_replicas ? g_count_synced_replicas(ws.target_offset) : 0;
      const bool timed_out = ws.deadline_ms > 0 && wloop_now >= ws.deadline_ms;
      if (synced >= ws.min_replicas || timed_out) {
        session.wait_state.reset();
        --wait_count;
        const std::string reply = ":" + std::to_string(synced) + "\r\n";
        if (!enqueue_output(wfd, reply)) to_close.push_back(wfd);
        else to_drain.push_back(wfd);
      }
    }
    for (int cfd : to_close) close_client(cfd);
    std::vector<int> drain_close;
    drain_pipelined(to_drain, blocked_count, wait_count, drain_close);
    for (int cfd : drain_close) close_client(cfd);
  };

  auto flush_replication_stream = [&](int fd, SessionState& session) {
    if (!session.replica_stream) return true;
    const std::size_t current_total = replication_event_count();
    if (session.repl_index < current_total) {
      const auto payload = replication_event_at(session.repl_index);
      if (!payload.empty() && !enqueue_output(fd, payload)) return false;
      session.repl_index = current_total;
    }
    return true;
  };

  auto handle_accept_fd_exhaustion = [&](int listener_fd, const std::string& name) {
    log(LogLevel::Warn, name + ": out of file descriptors, rejecting one pending connection");
    if (reserved_fd >= 0) {
      ::close(reserved_fd);
      reserved_fd = -1;
    }
    const int dropped_fd = accept_nonblock(listener_fd, nullptr, nullptr);
    if (dropped_fd >= 0) {
      ::close(dropped_fd);
    }
    reserved_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (reserved_fd < 0) {
      log(LogLevel::Warn, "failed to re-open reserved fd after EMFILE handling: " + std::string(std::strerror(errno)));
    }
  };

  while (!shutdown_requested()) {
    // Poll timeout: default 100 ms housekeeping tick, but clamp to the nearest blocked-
    // client deadline so BLPOP-style timeouts fire with millisecond precision instead
    // of up to a tick late (the upstream suite asserts ~150 ms slack on a 1 s timeout).
    int poll_ms = 100;
    if (blocked_count > 0) {
      const auto pnow = DataStore::now_ms();
      for (const auto& [bfd, bsession] : sessions) {
        if (bsession.blocked.type == BlockType::None || bsession.blocked.deadline_ms <= 0) continue;
        const auto delta = bsession.blocked.deadline_ms - pnow;
        if (delta < poll_ms) poll_ms = delta > 1 ? static_cast<int>(delta) : 1;
      }
    }
    const int nready = poller.wait(ep_events, poll_ms);
    if (nready < 0) {
      if (errno == EINTR) continue;
      log(LogLevel::Error, "poller wait failed: " + std::string(std::strerror(errno)));
      break;
    }

    const auto loop_now_ms = DataStore::now_ms();
    if (active_expire_enabled() && (loop_now_ms - last_active_expire_ms) >= 100) {
      run_active_expire_cycle();  // master expires + propagates DEL; replica is a no-op
      last_active_expire_ms = loop_now_ms;
    }
    aof_fsync_if_needed(loop_now_ms);
    background_save_cron();
    run_eviction_cycle();  // background eviction when over maxmemory (e.g. lowered via CONFIG SET)
    reap_background_children();
    drain_replica_cmd_queue();

    // ── Idle-client timeout reaping ──────────────────────────────────────
    // Close normal clients idle longer than `timeout` seconds. Replicas, blocked/waiting
    // clients, in-MULTI clients, and pub/sub subscribers are exempt (they legitimately sit
    // quiet). Swept at most ~once/sec to keep it cheap. timeout=0 disables it.
    if (const int idle_to = client_timeout_seconds(); idle_to > 0 &&
        loop_now_ms - last_idle_reap_ms >= 1000) {
      last_idle_reap_ms = loop_now_ms;
      const std::int64_t cutoff = static_cast<std::int64_t>(idle_to) * 1000;
      std::vector<int> idle_fds;
      for (auto& [cfd, session] : sessions) {
        if (session.replica_stream || session.blocked.type != BlockType::None ||
            session.wait_state.has_value() || session.in_multi) continue;
        if (session.last_interaction_ms == 0) continue;  // never issued a command yet
        if (loop_now_ms - session.last_interaction_ms < cutoff) continue;
        if (client_has_subscriptions(cfd)) continue;
        idle_fds.push_back(cfd);
      }
      for (int cfd : idle_fds) close_client(cfd);
    }

    // ── Check blocked clients ──────────────────────────────────────────
    if (has_pending_wakeups()) clear_pending_wakeups();
    recheck_blocked();
    recheck_wait();

    for (int i = 0; i < nready; ++i) {
      const int fd = ep_events[i].fd;

      // Skip fds already closed (e.g. by blocked-client cleanup above).
      if (fd != listen_fd && fd != cbus_fd && fd != unix_fd &&
          !sessions.count(fd) && !cbus_buffers.count(fd)) {
        continue;
      }

      // ── Handle errors / hangups ──
      if (ep_events[i].error && fd != listen_fd && fd != cbus_fd && fd != unix_fd) {
        if (cbus_buffers.count(fd)) {
          poller.remove(fd);
          ::close(fd);
          cbus_buffers.erase(fd);
        } else {
          close_client(fd);
        }
        continue;
      }

      // ── Flush pending output on writable ──
      if (ep_events[i].writable && fd != listen_fd && fd != cbus_fd && fd != unix_fd) {
        if (!flush_output(fd)) {
          close_client(fd);
          continue;
        }
      }

      if (!ep_events[i].readable) continue;

      if (fd == listen_fd || (unix_fd >= 0 && fd == unix_fd)) {
        const bool is_unix = (unix_fd >= 0 && fd == unix_fd);
        // Accept all pending connections (non-blocking accept loop)
        while (true) {
          sockaddr_storage client_addr {};
          socklen_t len = sizeof(client_addr);
          const int client_fd = accept_nonblock(fd,
                                        reinterpret_cast<sockaddr*>(&client_addr), &len);
          if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EMFILE || errno == ENFILE) {
              handle_accept_fd_exhaustion(fd, "client listener");
              break;
            }
            log(LogLevel::Warn, "accept() failed: " + std::string(std::strerror(errno)));
            break;
          }

          if (g_connected_clients.load(std::memory_order_relaxed) >= max_clients) {
            static constexpr const char kErrMaxClients[] = "-ERR max number of clients reached\r\n";
            const ssize_t wn = ::write(client_fd, kErrMaxClients, sizeof(kErrMaxClients) - 1);
            (void)wn;
            ::close(client_fd);
            continue;
          }

          // ── protected-mode ── refuse non-loopback clients when no password is set,
          // so an accidentally network-exposed server is not wide open. Loopback
          // (127.0.0.0/8) and unix-socket clients are always allowed (like Redis).
          if (!is_unix && protected_mode_enabled() && !requirepass_is_set()) {
            const auto* in4 = reinterpret_cast<const sockaddr_in*>(&client_addr);
            const bool is_loopback =
                (ntohl(in4->sin_addr.s_addr) >> 24) == 127;
            if (!is_loopback) {
              static constexpr const char kDenied[] =
                  "-DENIED PeaDB is running in protected mode because protected mode is enabled, "
                  "no bind address was specified, and no authentication password is requested to "
                  "clients. Connect from localhost, set a 'requirepass', or disable protected mode "
                  "('protected-mode no').\r\n";
              const ssize_t wn = ::write(client_fd, kDenied, sizeof(kDenied) - 1);
              (void)wn;
              ::close(client_fd);
              continue;
            }
          }

          if (!is_unix) {
            set_tcp_nodelay(client_fd);
            set_tcp_keepalive(client_fd);
          }

          if (!poller.add_read(client_fd)) {
            log(LogLevel::Warn, "poller add failed for client fd=" + std::to_string(client_fd) +
                                  ": " + std::string(std::strerror(errno)));
            ::close(client_fd);
            continue;
          }

          buffers.emplace(client_fd, "");
          write_buffers.emplace(client_fd, WriteBuf{});
          {
            auto& sess = sessions[client_fd];
            sess = SessionState{};
            // clientSetDefaultAuth: a client is authenticated at connect time iff the
            // default user is nopass (no requirepass). This must be captured now, not
            // recomputed per command, so a later CONFIG SET requirepass does not retroactively
            // de-authenticate connections that joined while the server was open.
            sess.authenticated = !requirepass_is_set();
            sess.client_id = g_next_client_id.fetch_add(1);
            sess.fd = client_fd;
            if (is_unix) {
              sess.addr = config.unixsocket + ":0";  // Redis reports <socketpath>:0
            } else {
              const auto* in4 = reinterpret_cast<const sockaddr_in*>(&client_addr);
              char ipbuf[INET_ADDRSTRLEN] = {0};
              inet_ntop(AF_INET, &in4->sin_addr, ipbuf, sizeof(ipbuf));
              sess.addr = std::string(ipbuf) + ":" + std::to_string(ntohs(in4->sin_port));
            }
            const auto snow = DataStore::now_ms();
            sess.created_ms = snow;
            sess.last_interaction_ms = snow;
          }
          g_connected_clients.fetch_add(1);
          g_total_connections_received.fetch_add(1);
          log(LogLevel::Debug, "accepted client fd=" + std::to_string(client_fd));
        }
        continue;
      }

      // Handle cluster bus listener
      if (fd == cbus_fd) {
        while (true) {
          sockaddr_in cbus_client_addr {};
          socklen_t clen = sizeof(cbus_client_addr);
          const int cbus_client_fd = accept_nonblock(cbus_fd,
                                             reinterpret_cast<sockaddr*>(&cbus_client_addr), &clen);
          if (cbus_client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EMFILE || errno == ENFILE) {
              handle_accept_fd_exhaustion(cbus_fd, "cluster bus listener");
              break;
            }
            log(LogLevel::Warn, "cluster bus accept() failed: " + std::string(std::strerror(errno)));
            break;
          }

          if (!poller.add_read(cbus_client_fd)) {
            ::close(cbus_client_fd);
            continue;
          }

          cbus_buffers.emplace(cbus_client_fd, "");
          log(LogLevel::Info, "cluster bus accepted fd=" + std::to_string(cbus_client_fd));
        }
        continue;
      }

      // Handle cluster bus data (gossip protocol — simple PING/PONG)
      if (cbus_buffers.count(fd)) {
        char cbuf[4096];
        const ssize_t cn = read(fd, cbuf, sizeof(cbuf));
        if (cn <= 0) {
          if (cn < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) continue;
          poller.remove(fd);
          ::close(fd);
          cbus_buffers.erase(fd);
        } else {
          const char pong[] = "PONG";
          if (::write(fd, pong, 4) < 0) {
            // best-effort gossip reply; ignore failure
          }
        }
        continue;
      }

      // ── Client data ──
      char buf[65536];
      const ssize_t n = read(fd, buf, sizeof(buf));
      if (n <= 0) {
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) continue;
        close_client(fd);
        continue;
      }

      auto& input = buffers[fd];
      input.append(buf, static_cast<std::size_t>(n));

      // Cap the per-client query buffer so a slow/never-completing command (or a
      // flood that never forms a full frame) cannot grow input without bound and
      // OOM the single-process server. 1 GB leaves headroom above proto-max-bulk-len.
      constexpr std::size_t kInputBufHardLimit = 1ULL * 1024 * 1024 * 1024;
      if (input.size() > kInputBufHardLimit) {
        log(LogLevel::Warn, "client fd=" + std::to_string(fd) +
            " exceeded query buffer limit (" + std::to_string(input.size()) + " bytes); closing");
        close_client(fd);
        continue;
      }

      // Parse all complete commands, batching replies for a single write
      std::string batch_reply;
      bool should_close = false;
      auto& session = sessions[fd];
      const bool was_blocked = session.blocked.type != BlockType::None;
      const bool was_waiting = session.wait_state.has_value();
      const bool was_replica = session.replica_stream;

      std::size_t consumed_total = 0;
      while (true) {
        const std::string_view remaining(input.data() + consumed_total, input.size() - consumed_total);
        auto parsed = parse_one_command(remaining, requirepass_is_set() && !session.authenticated);
        if (!parsed.has_value()) break;

        g_total_commands_processed.fetch_add(1);
        self_dispatch_fd = session.fd;
        const std::string reply = safe_handle_command(parsed->args, session, should_close);
        batch_reply += reply;
        batch_reply += self_pubsub_pending;  // self-directed messages come after the reply
        self_pubsub_pending.clear();
        self_dispatch_fd = -1;
        consumed_total += parsed->consumed;

        if (should_close) break;
        // Stop parsing when the session transitions into a blocking/waiting state.
        // Subsequent pipelined commands must not overtake the deferred reply.
        if (session.blocked.type != BlockType::None || session.wait_state.has_value()) break;
      }

      // Track blocked/replica/wait state changes
      if (!was_blocked && session.blocked.type != BlockType::None) {
        ++blocked_count;
        static std::uint64_t block_seq = 0;
        session.blocked.seq = ++block_seq;  // FIFO serving order
      }
      if (!was_waiting && session.wait_state.has_value()) ++wait_count;
      if (!was_replica && session.replica_stream) ++replica_count;

      if (consumed_total > 0) {
        if (consumed_total == input.size()) {
          input.clear();
        } else {
          input.erase(0, consumed_total);
        }
      }

      // Write all batched replies at once
      if (!batch_reply.empty()) {
        if (!enqueue_output(fd, batch_reply)) {
          close_client(fd);
          continue;
        }
      }

      if (!flush_replication_stream(fd, session)) {
        close_client(fd);
        continue;
      }

      if (should_close) {
        close_client(fd);
        continue;
      }
    }

    // Flush replication streams for all active sessions.
    if (replica_count > 0) {
      std::vector<int> to_close;
      for (auto& [rfd, session] : sessions) {
        if (!flush_replication_stream(rfd, session)) {
          to_close.push_back(rfd);
        }
      }
      for (int cfd : to_close) close_client(cfd);
    }

    // Same-iteration unblock: writes processed above set the pending-wakeup flag; deliver
    // to blocked/WAITing clients now rather than waiting for the next ~100ms poll. This is
    // what removes the blocking-command latency floor.
    if (has_pending_wakeups()) {
      clear_pending_wakeups();
      recheck_blocked();
      recheck_wait();
    }

    // Close clients whose output buffer overflowed its class limit during an async send.
    if (!obuf_overflow_fds.empty()) {
      for (int cfd : obuf_overflow_fds) {
        log(LogLevel::Warn, "closing client fd=" + std::to_string(cfd) +
                            " (client-output-buffer-limit exceeded)");
        close_client(cfd);
      }
      obuf_overflow_fds.clear();
    }
  }

  // Cleanup
  for (auto& [fd, _] : sessions) ::close(fd);
  for (auto& [fd, _] : cbus_buffers) ::close(fd);
  close(listen_fd);
  if (unix_fd >= 0) {
    close(unix_fd);
    ::unlink(config.unixsocket.c_str());
  }
  if (cbus_fd >= 0) close(cbus_fd);
  if (reserved_fd >= 0) close(reserved_fd);
  poller.destroy();
  return 0;
}

}  // namespace peadb
