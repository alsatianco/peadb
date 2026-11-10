#include "replication_load.hpp"

#include "datastore.hpp"

#include <algorithm>
#include <cerrno>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace peadb {
ReplicationLoadStrategy replication_load_strategy(ReplicationLoadMode mode, DataStore& data,
                                                  std::string_view current_id,
                                                  std::string_view incoming_id) {
  if (mode == ReplicationLoadMode::Disabled) return ReplicationLoadStrategy::Disk;
  if (mode == ReplicationLoadMode::OnEmptyDb) {
    struct RestoreDb {
      DataStore& data;
      int selected;
      ~RestoreDb() {
        data.select_db(selected);
      }
    } restore{data, data.current_db()};
    for (std::size_t db = 0; db < data.database_count(); ++db) {
      data.select_db(static_cast<int>(db));
      if (data.dbsize() != 0) return ReplicationLoadStrategy::Disk;
    }
    return ReplicationLoadStrategy::DirectBlocking;
  }
  return current_id.size() == 40 && current_id == incoming_id
             ? ReplicationLoadStrategy::DirectAsync
             : ReplicationLoadStrategy::DirectBlocking;
}

ReplicationLoadSession::ReplicationLoadSession(int socket, ReplicationLoadIdentity id)
    : identity(id), socket_(socket) {
}
ReplicationLoadSession::~ReplicationLoadSession() {
  if (socket_ >= 0) ::close(socket_);
}
bool ReplicationLoadSession::offer() {
  std::lock_guard lock(mutex_);
  if (std::this_thread::get_id() != producer_ || state_ != State::Link || socket_ < 0) return false;
  state_ = State::Offered;
  return true;
}
void ReplicationLoadSession::cancel() {
  std::lock_guard lock(mutex_);
  cancelled_ = true;
  if (state_ != State::Loading) {
    state_ = State::Returned;
    result_ = {};
    completed_.notify_all();
  }
}
bool ReplicationLoadSession::cancelled() const {
  std::lock_guard lock(mutex_);
  return cancelled_;
}
ReplicationLoadResult ReplicationLoadSession::wait(const std::function<bool()>& current) {
  std::unique_lock lock(mutex_);
  while (state_ != State::Returned) {
    if (current) {
      // User callbacks may inspect/cancel this session; never invoke them under its lock.
      lock.unlock();
      const bool proceed = current();
      lock.lock();
      if (!proceed) {
        lock.unlock();
        cancel();
        lock.lock();
      }
    }
    if (state_ != State::Returned) completed_.wait_for(lock, std::chrono::milliseconds(10));
  }
  return result_;
}
int ReplicationLoadSession::take_socket() {
  std::lock_guard lock(mutex_);
  if (std::this_thread::get_id() != producer_ || state_ != State::Returned) return -1;
  return std::exchange(socket_, -1);
}
ReplicationLoadLease::ReplicationLoadLease(ReplicationLoadSession& session,
                                           ReplicationLoadIdentity expected,
                                           std::uint64_t current_generation)
    : session_(session) {
  std::lock_guard lock(session_.mutex_);
  if (session_.identity != expected || expected.generation != current_generation ||
      session_.state_ != ReplicationLoadSession::State::Offered || session_.cancelled_)
    return;
  session_.state_ = ReplicationLoadSession::State::Loading;
  session_.reader_ = owner_;
  active_ = true;
}
ReplicationLoadLease::~ReplicationLoadLease() {
  finish(false);
}
ReplicationLoadLease::operator bool() const {
  return std::this_thread::get_id() == owner_ && active_;
}
void ReplicationLoadLease::finish(bool ok, std::string read_ahead) {
  if (std::this_thread::get_id() != owner_ || !active_) return;
  std::lock_guard lock(session_.mutex_);
  session_.result_.ok = ok && !session_.cancelled_ && read_ahead.size() <= 65536;
  if (session_.result_.ok) session_.result_.read_ahead = std::move(read_ahead);
  session_.state_ = ReplicationLoadSession::State::Returned;
  active_ = false;
  session_.completed_.notify_all();
}
int ReplicationLoadLease::socket(std::uint64_t current_generation) const {
  std::lock_guard lock(session_.mutex_);
  if (std::this_thread::get_id() != owner_ || !active_ || session_.reader_ != owner_ ||
      session_.state_ != ReplicationLoadSession::State::Loading || session_.cancelled_ ||
      current_generation != session_.identity.generation)
    return -1;
  return session_.socket_;
}
ReplicationSocketSource::ReplicationSocketSource(ReplicationLoadLease& lease,
                                                 std::function<std::uint64_t()> generation,
                                                 Timeout timeout, std::function<bool()> service)
    : lease_(lease),
      generation_(std::move(generation)),
      timeout_(std::move(timeout)),
      service_(std::move(service)) {
}
ssize_t ReplicationSocketSource::read(void* buffer, std::size_t size) {
  if (size == 0) return 0;
  for (;;) {
    if (lease_.socket(generation_()) < 0) {
      errno = ECANCELED;
      return -1;
    }
    // Check again after service: it may cancel or replace this generation.
    if (!service_()) {
      errno = ECANCELED;
      return -1;
    }
    const int fd = lease_.socket(generation_());
    if (fd < 0) {
      errno = ECANCELED;
      return -1;
    }
    const ssize_t n = ::recv(fd, buffer, std::min<std::size_t>(size, 65536), MSG_DONTWAIT);
    const int read_errno = errno;
    if (n >= 0) {
      if (n > 0) last_io_ = std::chrono::steady_clock::now();
      return n;
    }
    if (read_errno == EINTR) continue;
    if (read_errno != EAGAIN && read_errno != EWOULDBLOCK) {
      errno = read_errno;
      return -1;
    }
    const auto remaining = timeout_() - std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::steady_clock::now() - last_io_);
    if (remaining.count() <= 0) {
      errno = ETIMEDOUT;
      return -1;
    }
    pollfd p{fd, POLLIN, 0};
    const int ready =
        ::poll(&p, 1, static_cast<int>(std::min<std::int64_t>(remaining.count(), 10)));
    if (ready < 0 && errno != EINTR) return -1;
    // recv reports EOF, HUP and socket errors; a timeout/EINTR services clients again.
  }
}
}  // namespace peadb
