#include "replication_load.hpp"
#include "datastore.hpp"
#include "functions.hpp"
#include "rdb.hpp"
#include "lua_engine.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <future>
#include <iostream>
#include <stdexcept>
#include <csignal>
#include <pthread.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>

namespace {
std::atomic<int> failures{0};
void check(bool ok, const char* message) {
  if (!ok) {
    ++failures;
    std::cerr << "FAIL: " << message << '\n';
  }
}
struct Pair {
  int fd[2];
  Pair() {
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fd) != 0) throw std::runtime_error("socketpair");
  }
  ~Pair() {
    for (int f : fd)
      if (f >= 0) ::close(f);
  }
  int take() {
    return std::exchange(fd[0], -1);
  }
};

void ownership() {
  using namespace peadb;
  Pair pair;
  ReplicationLoadSession session(pair.take(), {7, 11});
  check(session.offer(), "offer socket");
  check(!session.offer(), "cannot offer twice");
  {
    ReplicationLoadLease wrong(session, {7, 12}, 7);
    check(!wrong, "wrong attempt cannot claim socket");
  }
  auto consumer = std::async(std::launch::async, [&] {
    ReplicationLoadLease lease(session, {7, 11}, 7);
    check(bool(lease), "event loop claims socket");
    ReplicationLoadLease duplicate(session, {7, 11}, 7);
    check(!duplicate, "no second reader");
    check(session.take_socket() == -1, "consumer cannot take socket from producer");
    session.cancel();
    check(session.cancelled(), "cancellation visible during lease");
    lease.finish(true, "must not resume");
  });
  const auto result = session.wait();
  consumer.get();
  check(!result.ok && result.read_ahead.empty(), "cancelled completion cannot return commands");
  int fd = session.take_socket();
  check(fd >= 0 && session.take_socket() == -1, "producer reclaims exactly once");
  ::close(fd);

  Pair second;
  ReplicationLoadSession stale(second.take(), {8, 1});
  stale.offer();
  ReplicationLoadLease rejected(stale, {8, 1}, 9);
  check(!rejected, "superseded generation rejected");
  stale.cancel();
  check(!stale.wait().ok, "unclaimed cancellation wakes producer");

  Pair third;
  ReplicationLoadSession excepted(third.take(), {9, 1});
  excepted.offer();
  try {
    ReplicationLoadLease lease(excepted, {9, 1}, 9);
    throw std::runtime_error("unwind");
  } catch (const std::runtime_error&) {
  }
  check(!excepted.wait().ok, "exception returns ownership with failure");

  Pair abandoned;
  const int owned = abandoned.fd[0];
  {
    ReplicationLoadSession dropped(abandoned.take(), {10, 1});
    dropped.offer();
    dropped.cancel();
  }
  check(::fcntl(owned, F_GETFD) == -1 && errno == EBADF, "abandoned session closes owned socket");
}

void source() {
  using namespace peadb;
  using namespace std::chrono;
  Pair pair;
  const int owned = pair.fd[0];
  const int flags = ::fcntl(owned, F_GETFL);
  ReplicationLoadSession session(pair.take(), {1, 2});
  session.offer();
  ReplicationLoadLease lease(session, {1, 2}, 1);
  std::uint64_t generation = 1;
  int services = 0;
  bool proceed = true;
  milliseconds timeout(60);
  ReplicationSocketSource input(
      lease, [&] { return generation; }, [&] { return timeout; },
      [&] {
        ++services;
        errno = ENOTTY;  // checkpoint bookkeeping cannot replace a socket error.
        return proceed;
      });
  char buffer[100];
  auto worker = std::async(std::launch::async,
                           [&] { return input.read(buffer, 1) == -1 && errno == ECANCELED; });
  check(worker.get() && services == 0, "worker cannot read or service event-loop callbacks");
  const auto start = steady_clock::now();
  check(input.read(buffer, sizeof(buffer)) == -1 && errno == ETIMEDOUT, "stalled input times out");
  check(services >= 3 && steady_clock::now() - start < seconds(2), "bounded wait services events");
  check(::send(pair.fd[1], "abc", 3, 0) == 3, "send source bytes");
  check(input.read(buffer, sizeof(buffer)) == 3 && std::string(buffer, 3) == "abc",
        "available progress resets timeout");
  generation = 2;
  check(input.read(buffer, 1) == -1 && errno == ECANCELED, "generation cancels source");
  generation = 1;
  proceed = false;
  check(input.read(buffer, 1) == -1 && errno == ECANCELED, "shutdown checkpoint cancels source");
  proceed = true;
  ::shutdown(pair.fd[1], SHUT_WR);
  check(input.read(buffer, 1) == 0, "socket EOF reaches decoder");
  lease.finish(true, "suffix");
  check(input.read(buffer, 1) == -1 && errno == ECANCELED, "returned lease cannot read");
  const auto result = session.wait();
  check(result.ok && result.read_ahead == "suffix", "successful unread suffix returns exactly");
  check(::fcntl(owned, F_GETFL) == flags, "source preserves socket flags");
}

void source_controls() {
  using namespace peadb;
  using namespace std::chrono;
  Pair pair;
  ReplicationLoadSession session(pair.take(), {6, 1});
  session.offer();
  {
    ReplicationLoadLease lease(session, {6, 1}, 6);
    int services = 0;
    milliseconds timeout(500);
    ReplicationSocketSource input(
        lease, [] { return 6; }, [&] { return timeout; },
        [&] {
          if (++services == 3) timeout = milliseconds(25);
          return true;
        });
    char c;
    const auto start = steady_clock::now();
    check(input.read(&c, 1) == -1 && errno == ETIMEDOUT && services >= 3 &&
              steady_clock::now() - start < milliseconds(400),
          "live timeout change applies during stalled wait");
    ReplicationSocketSource cancelled(
        lease, [] { return 6; }, [&] { return seconds(1); },
        [&] {
          session.cancel();
          return true;
        });
    check(cancelled.read(&c, 1) == -1 && errno == ECANCELED,
          "callback cancellation rechecked before reading");
  }
  check(!session.wait().ok, "cancelled reader unwinds to failure");

  Pair excepted;
  ReplicationLoadSession throwing(excepted.take(), {6, 2});
  throwing.offer();
  try {
    ReplicationLoadLease lease(throwing, {6, 2}, 6);
    ReplicationSocketSource input(
        lease, [] { return 6; }, [] { return seconds(1); },
        []() -> bool { throw std::runtime_error("service exception"); });
    char c;
    input.read(&c, 1);
    check(false, "service exception must propagate");
  } catch (const std::runtime_error&) {
  }
  check(!throwing.wait().ok, "throwing service cannot strand socket ownership");
}

void cancellation_wait() {
  using namespace peadb;
  Pair pair;
  ReplicationLoadSession session(pair.take(), {2, 4});
  session.offer();
  std::promise<void> claimed, release;
  auto ready = claimed.get_future();
  auto release_ready = release.get_future();
  auto consumer = std::async(std::launch::async, [&] {
    ReplicationLoadLease lease(session, {2, 4}, 2);
    claimed.set_value();
    release_ready.wait();
    lease.finish(true);
  });
  ready.wait();
  session.cancel();
  auto waiter = std::async(std::launch::async, [&] { return session.wait(); });
  check(waiter.wait_for(std::chrono::milliseconds(30)) == std::future_status::timeout,
        "producer stays paused until cancelled lease unwinds");
  check(session.take_socket() == -1, "cancellation cannot close a live reader's fd");
  release.set_value();
  check(!waiter.get().ok, "cancel wins over claimed success");
  consumer.get();

  Pair queued;
  ReplicationLoadSession unclaimed(queued.take(), {3, 1});
  unclaimed.offer();
  check(!unclaimed
             .wait([&] {
               unclaimed.cancel();
               return false;
             })
             .ok,
        "superseded queued producer can leave without callback deadlock");
  ReplicationLoadLease late(unclaimed, {3, 1}, 3);
  check(!late, "late consumer cannot reuse cancelled attempt");
}

volatile sig_atomic_t interrupted = 0;
void interrupt(int) {
  interrupted = 1;
}
void interrupted_input() {
  using namespace peadb;
  Pair pair;
  ReplicationLoadSession session(pair.take(), {4, 1});
  session.offer();
  ReplicationLoadLease lease(session, {4, 1}, 4);
  struct sigaction action {
  }, previous{};
  action.sa_handler = interrupt;
  sigemptyset(&action.sa_mask);
  sigaction(SIGUSR1, &action, &previous);
  const auto reader = pthread_self();
  auto writer = std::async(std::launch::async, [&] {
    for (int i = 0; i < 4; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      pthread_kill(reader, SIGUSR1);
    }
    return ::send(pair.fd[1], "ok", 2, 0);
  });
  ReplicationSocketSource input(
      lease, [] { return 4; }, [] { return std::chrono::seconds(2); }, [] { return true; });
  char bytes[2];
  check(input.read(bytes, 2) == 2 && std::string(bytes, 2) == "ok",
        "EINTR resumes real socket input");
  check(writer.get() == 2 && interrupted, "actual signal delivered during socket wait");
  sigaction(SIGUSR1, &previous, nullptr);
}

std::string bulk(const std::string& value) {
  const auto n = value.size();
  return std::string{char(0x80), char(n >> 24), char(n >> 16), char(n >> 8), char(n)} + value;
}
void framed_source(bool eof, bool corrupt) {
  using namespace peadb;
  Pair pair;
  const std::string marker(40, 'm'), suffix = "*2\r\n$4\r\nINCR\r\n$1\r\nk\r\n";
  const std::string value = std::string("binary\0", 7) + marker + std::string(200000, 'x');
  std::string rdb = "REDIS0011" + std::string(1, char(0)) + bulk("key") + bulk(value) + char(255);
  const auto crc = crc64(0, rdb.data(), rdb.size());
  for (int i = 0; i < 8; ++i) rdb += char(crc >> (8 * i));
  if (corrupt) rdb.back() ^= 1;
  const auto wire = rdb + (eof ? marker : "") + suffix;
  std::promise<std::shared_ptr<ReplicationLoadSession>> offered;
  auto available = offered.get_future();
  auto producer = std::async(std::launch::async, [&] {
    auto session =
        std::make_shared<ReplicationLoadSession>(pair.take(), ReplicationLoadIdentity{5, 8});
    session->offer();
    offered.set_value(session);
    const auto result = session->wait();
    int fd = session->take_socket();
    std::string commands = result.read_ahead;
    if (result.ok) {
      char bytes[256];
      while (commands.size() < suffix.size()) {
        const auto n = ::recv(fd, bytes, sizeof(bytes), 0);
        if (n <= 0) break;
        commands.append(bytes, static_cast<std::size_t>(n));
      }
    }
    ::close(fd);
    return std::pair{result.ok, commands};
  });
  auto writer = std::async(std::launch::async, [&] {
    std::size_t pos = 0;
    while (pos < wire.size()) {
      const auto n =
          ::send(pair.fd[1], wire.data() + pos, std::min<std::size_t>(997, wire.size() - pos), 0);
      if (n <= 0) return false;
      pos += static_cast<std::size_t>(n);
    }
    ::shutdown(pair.fd[1], SHUT_WR);
    return true;
  });
  auto session = available.get();
  {
    ReplicationLoadLease lease(*session, {5, 8}, 5);
    int services = 0;
    ReplicationSocketSource input(
        lease, [] { return 5; }, [] { return std::chrono::seconds(2); },
        [&] {
          ++services;
          return true;
        });
    DataStore staging;
    FunctionRegistry functions;
    RdbLoadStats stats;
    RdbLoadTarget target{staging, functions, stats};
    RdbStreamFrame frame{eof ? RdbStreamFrame::Kind::Eof : RdbStreamFrame::Kind::Length, rdb.size(),
                         eof ? marker : ""};
    RdbStreamResult result;
    std::string error;
    const bool ok = rdb_load_stream_into([&](void* p, std::size_t n) { return input.read(p, n); },
                                         frame, target, result, error);
    if (ok != !corrupt)
      std::cerr << "framed source: eof=" << eof << " corrupt=" << corrupt << " " << error << '\n';
    check(ok == !corrupt && services > 0, "real leased socket feeds common framed decoder");
    if (ok)
      check(staging.get("key") == value, "binary delimiter is data and exact value survives");
    else
      check(result.read_ahead.empty(), "bad checksum cannot publish command suffix");
    lease.finish(ok, std::move(result.read_ahead));
  }
  check(writer.get() || corrupt, "fragmented producer completed or refused corrupt frame");
  const auto [ok, commands] = producer.get();
  check(ok == !corrupt && commands == (corrupt ? "" : suffix),
        "producer resumes exact command boundary once");
}

void strategies() {
  using namespace peadb;
  DataStore data;
  const std::string id(40, 'a'), other(40, 'b');
  using Mode = ReplicationLoadMode;
  using Strategy = ReplicationLoadStrategy;
  check(replication_load_strategy(Mode::Disabled, data, id, id) == Strategy::Disk,
        "disabled always spools");
  check(replication_load_strategy(Mode::OnEmptyDb, data, id, id) == Strategy::DirectBlocking,
        "empty database loads directly with blocking admission");
  data.select_db(15);
  data.set("expired", "still physically stored", false, false, 1, false);
  data.select_db(3);
  check(replication_load_strategy(Mode::OnEmptyDb, data, id, id) == Strategy::Disk,
        "expired key in another database prevents empty strategy");
  check(data.current_db() == 3, "strategy preserves selected database");
  data.set_expire_paused(true);
  data.select_db(15);
  check(!data.get("expired"), "expired fixture hidden by paused expiry");
  data.select_db(3);
  check(replication_load_strategy(Mode::OnEmptyDb, data, id, id) == Strategy::Disk,
        "paused hidden key still prevents empty strategy");
  check(replication_load_strategy(Mode::SwapDb, data, id, id) == Strategy::DirectAsync,
        "matching ID swap permits old dataset service");
  check(replication_load_strategy(Mode::SwapDb, data, id, other) == Strategy::DirectBlocking,
        "changed ID swap uses blocking admission");
  check(replication_load_strategy(Mode::SwapDb, data, "?", "?") == Strategy::DirectBlocking,
        "unknown IDs cannot enable async admission");
}
}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  if (argc == 5 && std::string(argv[1]) == "--strategy") {
    using namespace peadb;
    DataStore data;
    if (std::string(argv[3]) != "empty") {
      data.select_db(15);
      data.set("old", "v", false, false, {}, false);
    }
    const auto mode = std::string(argv[2]) == "disabled"      ? ReplicationLoadMode::Disabled
                      : std::string(argv[2]) == "on-empty-db" ? ReplicationLoadMode::OnEmptyDb
                                                              : ReplicationLoadMode::SwapDb;
    const auto result =
        replication_load_strategy(mode, data, std::string(40, 'a'),
                                  std::string(40, std::string(argv[4]) == "matching" ? 'a' : 'b'));
    std::cout << (result == ReplicationLoadStrategy::Disk          ? "disk"
                  : result == ReplicationLoadStrategy::DirectAsync ? "async"
                                                                   : "blocking")
              << '\n';
    return 0;
  }
  peadb::lua_engine_init();
  ownership();
  source();
  source_controls();
  cancellation_wait();
  interrupted_input();
  for (bool eof : {false, true})
    for (bool corrupt : {false, true}) framed_source(eof, corrupt);
  strategies();
  peadb::lua_engine_shutdown();
  return failures ? 1 : 0;
}
