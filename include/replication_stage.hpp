#pragma once

#include "datastore.hpp"
#include "functions.hpp"
#include "rdb.hpp"
#include "replication_load.hpp"

#include <optional>

namespace peadb {

// Single-use private decoder target. Construct on the Lua owner thread BEFORE decoding;
// live objects and the retirement queue must outlive it. All methods/destruction stay on
// that thread, outside command/Lua execution and without outstanding target references.
// Preparation may throw, but cannot mutate live data/functions. Discard (including unwind)
// detaches partial data and callbacks to their respective prepared retirement queues.
// The caller drains functions outside Lua and destroys the queue before engine shutdown.
//
// This exchanges ONLY dataset/functions: it is not the server publication boundary.
// The caller must first prepare/coordinate history, watches, blockers and persistence,
// then publish between commands without yielding, and only then return the socket/result.
// No production replication strategy is activated by this internal type.
class ReplicationLoadStage {
 public:
  ReplicationLoadStage(DataStore& live, FunctionRegistry& functions,
                       FunctionRetirementQueue& retirement, ReplicationLoadIdentity identity);
  ~ReplicationLoadStage();
  ReplicationLoadStage(const ReplicationLoadStage&) = delete;
  ReplicationLoadStage& operator=(const ReplicationLoadStage&) = delete;

  bool load(const RdbReadFn& source, const RdbStreamFrame& frame, std::string& err,
            const RdbLoadControl* control = nullptr);
  // Only one complete validated frame with the exact current generation AND attempt may
  // exchange. A stale identity permanently discards the stage. No allocation/free, Lua
  // calls, key/function traversal or callbacks occur in publish()/discard().
  bool publish(ReplicationLoadIdentity current) noexcept;
  void discard() noexcept;
  // Prospective outputs for fallible history preparation; meaningful after load succeeds.
  const RdbLoadStats& stats() const;
  const RdbReplicationInfo& replication() const;
  // Unread commands remain private until paired exchange; transfer them at most once.
  std::optional<RdbStreamResult> take_result() noexcept;

 private:
  void check_owner() const noexcept;
  enum class State { Fresh, Failed, Ready, Published, Released, Discarded };
  const std::thread::id owner_ = std::this_thread::get_id();
  const ReplicationLoadIdentity identity_;
  DataStore& live_;
  FunctionRegistry& live_functions_;
  FunctionRetirementQueue& retirement_;
  DataStore data_;
  FunctionRegistry functions_;
  std::unique_ptr<DataStore::DatasetRetirement> data_ticket_;
  std::unique_ptr<FunctionRetirementQueue::Ticket> function_ticket_;
  RdbLoadStats stats_;
  RdbReplicationInfo replication_;
  RdbStreamResult result_;
  State state_ = State::Fresh;
};
}  // namespace peadb
