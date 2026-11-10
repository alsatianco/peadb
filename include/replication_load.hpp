#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <thread>

namespace peadb {
class DataStore;

enum class ReplicationLoadMode { Disabled, OnEmptyDb, SwapDb };
enum class ReplicationLoadStrategy { Disk, DirectBlocking, DirectAsync };
// Redis useDisklessLoad()/readSyncBulkPayload(): count physical keys in ALL databases,
// without expiry, then permit async admission only for a matching, known full replication ID.
// Main-thread only; this internal decision does not enable the public direct-load setting.
ReplicationLoadStrategy replication_load_strategy(ReplicationLoadMode mode, DataStore& data,
                                                  std::string_view current_id,
                                                  std::string_view incoming_id);

struct ReplicationLoadIdentity {
  std::uint64_t generation, attempt;
  bool operator==(const ReplicationLoadIdentity&) const = default;
};
struct ReplicationLoadResult {
  bool ok = false;
  std::string read_ahead;
};
class ReplicationLoadLease;

// A single socket handoff. The creating link thread offers it, then waits WITHOUT reading,
// sending, shutting down or closing the fd. One event-loop lease may claim it. Cancellation
// cannot return a claimed fd until that lease unwinds. No shared global completion sequence.
// Destruction closes the owned fd; take_socket() transfers it once to the creating thread,
// only after completion. A queued cancelled session can outlive its producer safely.
class ReplicationLoadSession {
 public:
  ReplicationLoadSession(int socket, ReplicationLoadIdentity identity);
  ~ReplicationLoadSession();
  ReplicationLoadSession(const ReplicationLoadSession&) = delete;
  ReplicationLoadSession& operator=(const ReplicationLoadSession&) = delete;
  const ReplicationLoadIdentity identity;
  bool offer();
  void cancel();
  bool cancelled() const;
  ReplicationLoadResult wait(const std::function<bool()>& current = {});
  int take_socket();

 private:
  friend class ReplicationLoadLease;
  enum class State { Link, Offered, Loading, Returned };
  mutable std::mutex mutex_;
  std::condition_variable completed_;
  State state_ = State::Link;
  bool cancelled_ = false;
  int socket_;
  const std::thread::id producer_ = std::this_thread::get_id();
  std::thread::id reader_;
  ReplicationLoadResult result_;
};

// Exception-safe consumer ownership. The session must outlive this scope. No nested lease,
// different attempt, worker-thread read or late completion can operate on another session.
class ReplicationLoadLease {
 public:
  ReplicationLoadLease(ReplicationLoadSession& session, ReplicationLoadIdentity expected,
                       std::uint64_t current_generation);
  ~ReplicationLoadLease();
  ReplicationLoadLease(const ReplicationLoadLease&) = delete;
  ReplicationLoadLease& operator=(const ReplicationLoadLease&) = delete;
  explicit operator bool() const;
  void finish(bool ok, std::string read_ahead = {});

 private:
  friend class ReplicationSocketSource;
  int socket(std::uint64_t current_generation) const;
  ReplicationLoadSession& session_;
  const std::thread::id owner_ = std::this_thread::get_id();
  bool active_ = false;
};

// Bounded nonblocking RDB source for a lease held on the event-loop thread. No fd flags are
// changed and no input is buffered here. Service must use the guarded loading pump, return
// false on shutdown, and never recursively drain master queues. Callbacks may throw; the
// enclosing lease still returns ownership. Live inactivity timeout uses a monotonic clock.
// The caller owns decoder progress/key-delay callbacks, CRC/framing and target publication.
class ReplicationSocketSource {
 public:
  using Timeout = std::function<std::chrono::milliseconds()>;
  ReplicationSocketSource(ReplicationLoadLease& lease, std::function<std::uint64_t()> generation,
                          Timeout timeout, std::function<bool()> service);
  ssize_t read(void* buffer, std::size_t size);

 private:
  ReplicationLoadLease& lease_;
  std::function<std::uint64_t()> generation_;
  Timeout timeout_;
  std::function<bool()> service_;
  std::chrono::steady_clock::time_point last_io_ = std::chrono::steady_clock::now();
};
}  // namespace peadb
