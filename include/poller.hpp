#pragma once

#include <unordered_map>
#include <vector>

#ifdef __linux__
#include <sys/epoll.h>
#else
#include <sys/event.h>
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

}  // namespace peadb
