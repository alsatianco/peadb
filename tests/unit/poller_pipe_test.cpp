#include "poller.hpp"

#include <cstdlib>
#include <iostream>
#include <unistd.h>

void require(bool condition, const char* message) {
  if (!condition) {
    std::cerr << message << '\n';
    std::exit(1);
  }
}

int main() {
  peadb::Poller poller;
  require(poller.create(), "create poller");
  int pipe[2];
  require(::pipe(pipe) == 0, "create pipe");
  require(poller.add_read(pipe[0]), "register pipe");
  require(::write(pipe[1], "x", 1) == 1, "write pipe");
  std::vector<peadb::PollEvent> events;
  require(poller.wait(events, 100) == 1 && events[0].fd == pipe[0] && events[0].readable,
          "initial pipe readiness");
  require(poller.suspend_read(pipe[0]), "suspend pending read");
  ::close(pipe[1]);
  // In particular, epoll events=0 would still deliver HUP here on every poll.
  require(poller.wait(events, 20) == 0, "suspended HUP must not wake poller");
  require(poller.resume_read(pipe[0]), "resume read after HUP");
  require(poller.wait(events, 100) == 1 && events[0].fd == pipe[0], "resumed pending bytes");
  char byte;
  require(::read(pipe[0], &byte, 1) == 1 && byte == 'x', "read preserved byte");
  require(::read(pipe[0], &byte, 1) == 0, "read EOF separately");
  require(poller.suspend_read(pipe[0]), "suspend EOF filter");
  poller.remove(pipe[0]);
  const int old_fd = pipe[0];
  ::close(pipe[0]);
  require(::pipe(pipe) == 0, "replacement pipe");
  require(pipe[0] == old_fd, "exercise actual FD reuse");
  require(poller.add_read(pipe[0]), "register reused FD after suspended remove");
  require(poller.wait(events, 20) == 0, "no stale readiness for reused FD");
  require(::write(pipe[1], "y", 1) == 1, "replacement data");
  require(poller.wait(events, 100) == 1 && events[0].readable, "replacement readiness");
  poller.remove(pipe[0]);
  ::close(pipe[0]);
  ::close(pipe[1]);
  poller.destroy();
  std::cout << "pipe suspend, HUP, resume and FD reuse passed\n";
}
