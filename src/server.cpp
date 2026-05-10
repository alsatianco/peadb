#include "server.hpp"

#include "command.hpp"
#include "datastore.hpp"
#include "logger.hpp"
#include "poller.hpp"
#include "protocol.hpp"
#include "server_context.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

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

bool set_tcp_nodelay(int fd) {
  int yes = 1;
  return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == 0;
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
    log(LogLevel::Error, "bind() failed: " + std::string(std::strerror(errno)));
    close(listen_fd);
    return 1;
  }

  if (listen(listen_fd, 4096) != 0) {
    log(LogLevel::Error, "listen() failed: " + std::string(std::strerror(errno)));
    close(listen_fd);
    return 1;
  }

  int reserved_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
  if (reserved_fd < 0) {
    log(LogLevel::Warn, "failed to open reserved fd for EMFILE handling: " + std::string(std::strerror(errno)));
  }

  log(LogLevel::Info, "peadb-server listening on " + config.bind + ":" + std::to_string(config.port));

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
  if (cbus_fd >= 0) poller.add_read(cbus_fd);
  std::vector<PollEvent> ep_events;
  std::unordered_map<int, std::string> buffers;
  std::unordered_map<int, WriteBuf> write_buffers;
  std::unordered_map<int, SessionState> sessions;
  std::int64_t last_active_expire_ms = DataStore::now_ms();
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

  // Wire up the CLIENT LIST callback
  g_client_list_callback = [&]() -> std::string {
    std::string out;
    for (auto& [fd, sess] : sessions) {
      out += "id=" + std::to_string(sess.client_id);
      out += " fd=" + std::to_string(fd);
      out += " name=" + sess.client_name;
      out += " lib-name=" + sess.lib_name;
      out += " lib-ver=" + sess.lib_ver;
      out += " db=" + std::to_string(sess.db_index);
      out += " sub=0 psub=0 ssub=0";
      out += " multi=" + std::string(sess.in_multi ? std::to_string(static_cast<int>(sess.queued.size())) : "-1");
      out += " watch=" + std::to_string(sess.watched_digests.size());
      out += " qbuf=0 qbuf-free=32768 argv-mem=0 multi-mem=0 tot-mem=20512";
      out += " net-i=0 net-o=0 age=0 idle=0";
      std::string flags;
      if (sess.replica_stream) flags += "S";
      if (sess.blocked.type != BlockType::None) flags += "b";
      if (flags.empty()) flags = "N";
      out += " flags=" + flags;
      out += " cmd=unknown user=default redir=-1";
      out += " resp=" + std::to_string(static_cast<int>(sess.resp_version));
      out += "\n";
    }
    return out;
  };

  // Mirror callbacks into the ServerContext struct for future callers.
  server_ctx().callbacks.count_synced_replicas = g_count_synced_replicas;
  server_ctx().callbacks.client_list = g_client_list_callback;

  // Helper: enqueue output data for a client and attempt immediate non-blocking flush.
  // Hard output buffer limit: disconnect clients whose buffer exceeds 256 MB
  constexpr std::size_t kOutputBufHardLimit = 256ULL * 1024 * 1024;

  auto enqueue_output = [&](int fd, const std::string& data) -> bool {
    if (data.empty()) return true;
    auto& wbuf = write_buffers[fd];
    // Enforce output buffer hard limit before appending
    if (wbuf.remaining() + data.size() > kOutputBufHardLimit) return false;
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

  // Wire up the pub/sub send callback (must be after enqueue_output is defined)
  g_pubsub_send = [&](int fd, const std::string& data) -> bool {
    if (!sessions.count(fd)) return false;
    return enqueue_output(fd, data);
  };

  // Helper: close a client connection and clean up all associated state.
  auto close_client = [&](int fd) {
    pubsub_unsubscribe_all(fd);
    poller.remove(fd);
    ::close(fd);
    buffers.erase(fd);
    write_buffers.erase(fd);
    sessions.erase(fd);
    g_connected_clients.fetch_sub(1);
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
    const int nready = poller.wait(ep_events, 100);
    if (nready < 0) {
      if (errno == EINTR) continue;
      log(LogLevel::Error, "poller wait failed: " + std::string(std::strerror(errno)));
      break;
    }

    const auto loop_now_ms = DataStore::now_ms();
    if (active_expire_enabled() && (loop_now_ms - last_active_expire_ms) >= 100) {
      store().active_expire_cycle();
      last_active_expire_ms = loop_now_ms;
    }
    aof_fsync_if_needed(loop_now_ms);
    reap_background_children();

    // ── Check blocked clients ──────────────────────────────────────────
    if (has_pending_wakeups()) clear_pending_wakeups();
    if (blocked_count > 0) {
      std::vector<int> to_close;
      for (auto& [bfd, session] : sessions) {
        if (session.blocked.type == BlockType::None) continue;
        const std::string reply = try_unblock_client(session);
        if (!reply.empty()) {
          --blocked_count;
          if (!enqueue_output(bfd, reply)) {
            to_close.push_back(bfd);
          }
        }
      }
      for (int cfd : to_close) close_client(cfd);
    }

    // ── Resolve deferred WAIT commands ────────────────────────────────
    if (wait_count > 0) {
      const auto wloop_now = DataStore::now_ms();
      std::vector<int> to_close;
      std::vector<int> to_drain;
      for (auto& [wfd, session] : sessions) {
        if (!session.wait_state.has_value()) continue;
        auto& ws = session.wait_state.value();
        int synced = g_count_synced_replicas ? g_count_synced_replicas(ws.target_offset)
                                             : g_connected_replicas.load();
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

      // Drain pipelined commands that were buffered while WAIT was pending.
      // Without this, the commands sit in buffers[] until the next epoll read
      // event, causing a deadlock if the client is waiting for those replies.
      std::vector<int> drain_close;
      for (int wfd : to_drain) {
        if (!sessions.count(wfd)) continue;
        auto& session = sessions[wfd];
        auto& input = buffers[wfd];
        if (input.empty()) continue;
        std::string batch;
        bool sc = false;
        std::size_t consumed = 0;
        while (true) {
          const std::string_view rem(input.data() + consumed, input.size() - consumed);
          auto parsed = parse_one_command(rem);
          if (!parsed.has_value()) break;
          g_total_commands_processed.fetch_add(1);
          batch += handle_command(parsed->args, session, sc);
          consumed += parsed->consumed;
          if (sc || session.blocked.type != BlockType::None || session.wait_state.has_value()) break;
        }
        if (consumed > 0) {
          if (consumed == input.size()) input.clear();
          else input.erase(0, consumed);
        }
        if (!batch.empty() && !enqueue_output(wfd, batch)) drain_close.push_back(wfd);
        else if (sc) drain_close.push_back(wfd);
        if (session.blocked.type != BlockType::None) ++blocked_count;
        if (session.wait_state.has_value()) ++wait_count;
      }
      for (int cfd : drain_close) close_client(cfd);
    }

    for (int i = 0; i < nready; ++i) {
      const int fd = ep_events[i].fd;

      // Skip fds already closed (e.g. by blocked-client cleanup above).
      if (fd != listen_fd && fd != cbus_fd &&
          !sessions.count(fd) && !cbus_buffers.count(fd)) {
        continue;
      }

      // ── Handle errors / hangups ──
      if (ep_events[i].error && fd != listen_fd && fd != cbus_fd) {
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
      if (ep_events[i].writable && fd != listen_fd && fd != cbus_fd) {
        if (!flush_output(fd)) {
          close_client(fd);
          continue;
        }
      }

      if (!ep_events[i].readable) continue;

      if (fd == listen_fd) {
        // Accept all pending connections (non-blocking accept loop)
        while (true) {
          sockaddr_in client_addr {};
          socklen_t len = sizeof(client_addr);
          const int client_fd = accept_nonblock(listen_fd,
                                        reinterpret_cast<sockaddr*>(&client_addr), &len);
          if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EMFILE || errno == ENFILE) {
              handle_accept_fd_exhaustion(listen_fd, "client listener");
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

          set_tcp_nodelay(client_fd);

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
            sess.client_id = g_next_client_id.fetch_add(1);
            sess.fd = client_fd;
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
        auto parsed = parse_one_command(remaining);
        if (!parsed.has_value()) break;

        g_total_commands_processed.fetch_add(1);
        const std::string reply = handle_command(parsed->args, session, should_close);
        batch_reply += reply;
        consumed_total += parsed->consumed;

        if (should_close) break;
        // Stop parsing when the session transitions into a blocking/waiting state.
        // Subsequent pipelined commands must not overtake the deferred reply.
        if (session.blocked.type != BlockType::None || session.wait_state.has_value()) break;
      }

      // Track blocked/replica/wait state changes
      if (!was_blocked && session.blocked.type != BlockType::None) ++blocked_count;
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
  }

  // Cleanup
  for (auto& [fd, _] : sessions) ::close(fd);
  for (auto& [fd, _] : cbus_buffers) ::close(fd);
  close(listen_fd);
  if (cbus_fd >= 0) close(cbus_fd);
  if (reserved_fd >= 0) close(reserved_fd);
  poller.destroy();
  return 0;
}

}  // namespace peadb
