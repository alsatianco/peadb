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
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef __linux__
#include <sys/epoll.h>
#else
#include <sys/event.h>   // kqueue on macOS/BSD
#endif

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

struct PollEvent { int fd; bool readable; bool writable; bool error; };

struct Poller {
  int pfd = -1;
  bool create() {
#ifdef __linux__
    pfd = epoll_create1(0);
#else
    pfd = kqueue();
#endif
    return pfd >= 0;
  }
  void destroy() { if (pfd >= 0) { ::close(pfd); pfd = -1; } }
  bool add_read(int fd) {
#ifdef __linux__
    epoll_event ev{}; ev.events = EPOLLIN; ev.data.fd = fd;
    return epoll_ctl(pfd, EPOLL_CTL_ADD, fd, &ev) == 0;
#else
    struct kevent kev; EV_SET(&kev, fd, EVFILT_READ, EV_ADD|EV_ENABLE, 0, 0, nullptr);
    return kevent(pfd, &kev, 1, nullptr, 0, nullptr) == 0;
#endif
  }
  bool set_readwrite(int fd) {
#ifdef __linux__
    epoll_event ev{}; ev.events = EPOLLIN|EPOLLOUT; ev.data.fd = fd;
    return epoll_ctl(pfd, EPOLL_CTL_MOD, fd, &ev) == 0;
#else
    struct kevent kev[2];
    EV_SET(&kev[0], fd, EVFILT_READ, EV_ADD|EV_ENABLE, 0, 0, nullptr);
    EV_SET(&kev[1], fd, EVFILT_WRITE, EV_ADD|EV_ENABLE, 0, 0, nullptr);
    return kevent(pfd, kev, 2, nullptr, 0, nullptr) == 0;
#endif
  }
  bool set_read_only(int fd) {
#ifdef __linux__
    epoll_event ev{}; ev.events = EPOLLIN; ev.data.fd = fd;
    return epoll_ctl(pfd, EPOLL_CTL_MOD, fd, &ev) == 0;
#else
    struct kevent kev; EV_SET(&kev, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    kevent(pfd, &kev, 1, nullptr, 0, nullptr);
    return true;
#endif
  }
  void remove(int fd) {
#ifdef __linux__
    epoll_ctl(pfd, EPOLL_CTL_DEL, fd, nullptr);
#else
    struct kevent kev[2];
    EV_SET(&kev[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&kev[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    kevent(pfd, kev, 2, nullptr, 0, nullptr);
#endif
  }
  int wait(std::vector<PollEvent>& out, int timeout_ms) {
#ifdef __linux__
    thread_local std::vector<epoll_event> raw(1024);
    int n = epoll_wait(pfd, raw.data(), static_cast<int>(raw.size()), timeout_ms);
    if (n < 0) return n;
    out.resize(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
      out[i] = {raw[i].data.fd, bool(raw[i].events & EPOLLIN),
                bool(raw[i].events & EPOLLOUT), bool(raw[i].events & (EPOLLERR|EPOLLHUP))};
    }
    return n;
#else
    thread_local std::vector<struct kevent> raw(1024);
    struct timespec ts{timeout_ms/1000, (timeout_ms%1000)*1000000L};
    struct timespec* tsp = (timeout_ms >= 0) ? &ts : nullptr;
    int n = kevent(pfd, nullptr, 0, raw.data(), static_cast<int>(raw.size()), tsp);
    if (n < 0) return n;
    std::unordered_map<int, std::size_t> idx_map;
    out.clear();
    for (int i = 0; i < n; ++i) {
      int fd = static_cast<int>(raw[i].ident);
      auto it = idx_map.find(fd);
      std::size_t idx;
      if (it == idx_map.end()) { idx = out.size(); idx_map[fd] = idx; out.push_back({fd,false,false,false}); }
      else idx = it->second;
      if (raw[i].filter == EVFILT_READ)  out[idx].readable = true;
      if (raw[i].filter == EVFILT_WRITE) out[idx].writable = true;
      if (raw[i].flags & EV_ERROR) out[idx].error = true;
      // EV_EOF on read means peer closed — mark readable so the read()
      // returns 0 and the normal close path handles it gracefully.
      if ((raw[i].flags & EV_EOF) && raw[i].filter == EVFILT_READ) out[idx].readable = true;
    }
    return static_cast<int>(out.size());
#endif
  }
};

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
  poller.destroy();
  return 0;
}

}  // namespace peadb
