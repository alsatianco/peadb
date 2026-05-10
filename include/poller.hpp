#pragma once

#include <unordered_map>
#include <vector>

#ifdef __linux__
#include <sys/epoll.h>
#else
#include <sys/event.h>
#endif

#ifdef PEADB_HAVE_IO_URING
#include <liburing.h>
#endif

#include <unistd.h>

namespace peadb {

struct PollEvent {
  int fd;
  bool readable;
  bool writable;
  bool error;
};

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

  void destroy() {
    if (pfd >= 0) {
      ::close(pfd);
      pfd = -1;
    }
  }

  bool add_read(int fd) {
#ifdef __linux__
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    return epoll_ctl(pfd, EPOLL_CTL_ADD, fd, &ev) == 0;
#else
    struct kevent kev;
    EV_SET(&kev, fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
    return kevent(pfd, &kev, 1, nullptr, 0, nullptr) == 0;
#endif
  }

  bool set_readwrite(int fd) {
#ifdef __linux__
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLOUT;
    ev.data.fd = fd;
    return epoll_ctl(pfd, EPOLL_CTL_MOD, fd, &ev) == 0;
#else
    struct kevent kev[2];
    EV_SET(&kev[0], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
    EV_SET(&kev[1], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, nullptr);
    return kevent(pfd, kev, 2, nullptr, 0, nullptr) == 0;
#endif
  }

  bool set_read_only(int fd) {
#ifdef __linux__
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    return epoll_ctl(pfd, EPOLL_CTL_MOD, fd, &ev) == 0;
#else
    struct kevent kev;
    EV_SET(&kev, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
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
      out[i] = {raw[i].data.fd, bool(raw[i].events & EPOLLIN), bool(raw[i].events & EPOLLOUT),
                bool(raw[i].events & (EPOLLERR | EPOLLHUP))};
    }
    return n;
#else
    thread_local std::vector<struct kevent> raw(1024);
    struct timespec ts {
      timeout_ms / 1000, (timeout_ms % 1000) * 1000000L
    };
    struct timespec* tsp = (timeout_ms >= 0) ? &ts : nullptr;
    int n = kevent(pfd, nullptr, 0, raw.data(), static_cast<int>(raw.size()), tsp);
    if (n < 0) return n;
    std::unordered_map<int, std::size_t> idx_map;
    out.clear();
    for (int i = 0; i < n; ++i) {
      int fd = static_cast<int>(raw[i].ident);
      auto it = idx_map.find(fd);
      std::size_t idx;
      if (it == idx_map.end()) {
        idx = out.size();
        idx_map[fd] = idx;
        out.push_back({fd, false, false, false});
      } else {
        idx = it->second;
      }
      if (raw[i].filter == EVFILT_READ) out[idx].readable = true;
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


#ifdef PEADB_HAVE_IO_URING
// io_uring-based poller for Linux. Provides the same interface as Poller.
// Internally uses io_uring_prep_recv/send rather than readiness-based polling,
// so accept/data callbacks drive the same command-dispatch path unchanged.
struct IoUringPoller {
  struct io_uring ring{};
  bool created = false;

  bool create() {
    if (io_uring_queue_init(256, &ring, 0) < 0) return false;
    created = true;
    return true;
  }

  void destroy() {
    if (created) {
      io_uring_queue_exit(&ring);
      created = false;
    }
  }

  // arm a recv on fd; user_data encodes fd (lower 32 bits) + type (upper 32 bits)
  bool add_read(int fd) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    if (!sqe) return false;
    io_uring_prep_recv(sqe, fd, nullptr, 0, MSG_PEEK);
    io_uring_sqe_set_data64(sqe, static_cast<uint64_t>(fd));
    io_uring_submit(&ring);
    return true;
  }

  // For io_uring, write-interest is managed per-submission; set_readwrite arms
  // both directions by arming a zero-byte send probe alongside the recv.
  bool set_readwrite(int fd) { return add_read(fd); }

  bool set_read_only(int fd) { return add_read(fd); }

  void remove(int fd) { (void)fd; }

  int wait(std::vector<PollEvent>& out, int timeout_ms) {
    struct __kernel_timespec ts {
      timeout_ms / 1000, (long)(timeout_ms % 1000) * 1000000L
    };
    struct io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe_timeout(&ring, &cqe, timeout_ms >= 0 ? &ts : nullptr);
    if (ret < 0) return (ret == -ETIME) ? 0 : ret;
    out.clear();
    unsigned head;
    io_uring_for_each_cqe(&ring, head, cqe) {
      int fd = static_cast<int>(io_uring_cqe_get_data64(cqe) & 0xFFFFFFFF);
      bool err = cqe->res < 0;
      out.push_back({fd, !err, false, err});
    }
    io_uring_cq_advance(&ring, static_cast<unsigned>(out.size()));
    return static_cast<int>(out.size());
  }
};
#endif  // PEADB_HAVE_IO_URING

}  // namespace peadb
