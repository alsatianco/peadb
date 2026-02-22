#include "server.hpp"

#include "command.hpp"
#include "datastore.hpp"
#include "logger.hpp"
#include "protocol.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
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

bool set_tcp_nodelay(int fd) {
  int yes = 1;
  return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == 0;
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
    data.erase(0, offset);
    offset = 0;
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

  const int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    log(LogLevel::Error, "epoll_create1() failed: " + std::string(std::strerror(errno)));
    close(listen_fd);
    if (cbus_fd >= 0) close(cbus_fd);
    return 1;
  }
  {
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);
    if (cbus_fd >= 0) {
      ev.data.fd = cbus_fd;
      epoll_ctl(epoll_fd, EPOLL_CTL_ADD, cbus_fd, &ev);
    }
  }
  std::vector<epoll_event> ep_events(1024);
  std::unordered_map<int, std::string> buffers;
  std::unordered_map<int, WriteBuf> write_buffers;
  std::unordered_map<int, SessionState> sessions;
  std::int64_t last_active_expire_ms = DataStore::now_ms();
  int blocked_count = 0;
  int replica_count = 0;

  // Wire up the WAIT callback so it can count synced replicas
  g_count_synced_replicas = [&](std::int64_t target_offset) -> int {
    int count = 0;
    for (auto& [fd, sess] : sessions) {
      if (sess.replica_stream && sess.repl_ack_offset >= target_offset) ++count;
    }
    return count;
  };

  // Helper: enqueue output data for a client and attempt immediate non-blocking flush.
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
    if (wbuf.empty()) {
      wbuf.data.clear();
      wbuf.offset = 0;
    } else if (wbuf.offset > 65536) {
      wbuf.compact();
    }
    // Register EPOLLOUT interest if data remains in the write buffer.
    if (!wbuf.empty() && was_empty) {
      epoll_event ev{};
      ev.events = EPOLLIN | EPOLLOUT;
      ev.data.fd = fd;
      epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
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
    // Remove EPOLLOUT interest once the write buffer is fully drained.
    if (wbuf.empty()) {
      epoll_event ev{};
      ev.events = EPOLLIN;
      ev.data.fd = fd;
      epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
    }
    return true;
  };

  // Helper: close a client connection and clean up all associated state.
  auto close_client = [&](int fd) {
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    buffers.erase(fd);
    write_buffers.erase(fd);
    sessions.erase(fd);
    g_connected_clients.fetch_sub(1);
  };

  auto flush_replication_stream = [&](int fd, SessionState& session) {
    if (!session.replica_stream) return true;
    while (session.repl_index < replication_event_count()) {
      const auto payload = replication_event_at(session.repl_index);
      if (payload.empty()) break;
      if (!enqueue_output(fd, payload)) return false;
      ++session.repl_index;
    }
    return true;
  };

  auto handle_accept_fd_exhaustion = [&](int listener_fd, const std::string& name) {
    log(LogLevel::Warn, name + ": out of file descriptors, rejecting one pending connection");
    if (reserved_fd >= 0) {
      ::close(reserved_fd);
      reserved_fd = -1;
    }
    const int dropped_fd = ::accept4(listener_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (dropped_fd >= 0) {
      ::close(dropped_fd);
    }
    reserved_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (reserved_fd < 0) {
      log(LogLevel::Warn, "failed to re-open reserved fd after EMFILE handling: " + std::string(std::strerror(errno)));
    }
  };

  while (!shutdown_requested()) {
    const int nready = epoll_wait(epoll_fd, ep_events.data(),
                                  static_cast<int>(ep_events.size()), 100);
    if (nready < 0) {
      if (errno == EINTR) continue;
      log(LogLevel::Error, "epoll_wait() failed: " + std::string(std::strerror(errno)));
      break;
    }

    const auto loop_now_ms = DataStore::now_ms();
    if (active_expire_enabled() && (loop_now_ms - last_active_expire_ms) >= 100) {
      store().active_expire_cycle();
      last_active_expire_ms = loop_now_ms;
    }

    // ── Check blocked clients ──────────────────────────────────────────
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

    for (int i = 0; i < nready; ++i) {
      const int fd = ep_events[i].data.fd;
      const uint32_t revents = ep_events[i].events;

      // Skip fds already closed (e.g. by blocked-client cleanup above).
      if (fd != listen_fd && fd != cbus_fd &&
          !sessions.count(fd) && !cbus_buffers.count(fd)) {
        continue;
      }

      // ── Handle errors / hangups ──
      if ((revents & (EPOLLERR | EPOLLHUP)) && fd != listen_fd && fd != cbus_fd) {
        if (cbus_buffers.count(fd)) {
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
          ::close(fd);
          cbus_buffers.erase(fd);
        } else {
          close_client(fd);
        }
        continue;
      }

      // ── Flush pending output on EPOLLOUT ──
      if ((revents & EPOLLOUT) && fd != listen_fd && fd != cbus_fd) {
        if (!flush_output(fd)) {
          close_client(fd);
          continue;
        }
      }

      if (!(revents & EPOLLIN)) continue;

      if (fd == listen_fd) {
        // Accept all pending connections (non-blocking accept loop)
        while (true) {
          sockaddr_in client_addr {};
          socklen_t len = sizeof(client_addr);
          const int client_fd = accept4(listen_fd,
                                        reinterpret_cast<sockaddr*>(&client_addr), &len,
                                        SOCK_NONBLOCK | SOCK_CLOEXEC);
          if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EMFILE || errno == ENFILE) {
              handle_accept_fd_exhaustion(listen_fd, "client listener");
              break;
            }
            log(LogLevel::Warn, "accept4() failed: " + std::string(std::strerror(errno)));
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

          epoll_event cev{};
          cev.events = EPOLLIN;
          cev.data.fd = client_fd;
          if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &cev) != 0) {
            log(LogLevel::Warn, "epoll_ctl(ADD) failed for client fd=" + std::to_string(client_fd) +
                                  ": " + std::string(std::strerror(errno)));
            ::close(client_fd);
            continue;
          }

          buffers.emplace(client_fd, "");
          write_buffers.emplace(client_fd, WriteBuf{});
          sessions.emplace(client_fd, SessionState{});
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
          const int cbus_client_fd = accept4(cbus_fd,
                                             reinterpret_cast<sockaddr*>(&cbus_client_addr), &clen,
                                             SOCK_NONBLOCK | SOCK_CLOEXEC);
          if (cbus_client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EMFILE || errno == ENFILE) {
              handle_accept_fd_exhaustion(cbus_fd, "cluster bus listener");
              break;
            }
            log(LogLevel::Warn, "cluster bus accept4() failed: " + std::string(std::strerror(errno)));
            break;
          }

          epoll_event cev{};
          cev.events = EPOLLIN;
          cev.data.fd = cbus_client_fd;
          if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, cbus_client_fd, &cev) != 0) {
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
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
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
      }

      // Track blocked/replica state changes
      if (!was_blocked && session.blocked.type != BlockType::None) ++blocked_count;
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
  close(epoll_fd);
  return 0;
}

}  // namespace peadb
