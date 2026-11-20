#include "replication_stage.hpp"
#include "command.hpp"

#include <exception>
#include <utility>

namespace peadb {
ReplicationLoadStage::ReplicationLoadStage(DataStore& live, FunctionRegistry& functions,
                                           FunctionRetirementQueue& retirement,
                                           ReplicationLoadIdentity identity)
    : identity_(identity),
      live_(live),
      live_functions_(functions),
      retirement_(retirement),
      data_ticket_(live.prepare_dataset_retirement()),
      function_ticket_(retirement.prepare()) {
  // Validate the live registry owner even when the incoming frame has no libraries.
  (void)live_functions_.libraries();
}

ReplicationLoadStage::~ReplicationLoadStage() {
  discard();
}

void ReplicationLoadStage::check_owner() const noexcept {
  if (owner_ != std::this_thread::get_id()) std::terminate();
}

bool ReplicationLoadStage::load(const RdbReadFn& source, const RdbStreamFrame& frame,
                                std::string& err, const RdbLoadControl* control) {
  check_owner();
  if (state_ != State::Fresh) {
    err = "replication load stage already used";
    return false;
  }
  // Mark before any fallible work: an exception must never leave a publishable stage.
  state_ = State::Failed;
  RdbLoadTarget target{data_, functions_, stats_};
  target.expiry = RdbLoadTarget::Expiry::KeepExpired;
  target.replication = &replication_;
  target.control = control;
  target.checksum = rdb_checksum();
  target.skip_checksum_validation = rdb_skip_checksum_validation();
  if (!rdb_load_stream_into(source, frame, target, result_, err)) return false;
  state_ = State::Ready;
  return true;
}

bool ReplicationLoadStage::publish(ReplicationLoadIdentity current,
                                   std::span<SessionState* const> clients) noexcept {
  check_owner();
  if (state_ != State::Ready) return false;
  if (current != identity_) {
    discard();
    return false;
  }
  // Redis multi.c touchAllWatchedKeysInDb, with one publication-time clock sample.
  // Inspect physical keys without constructing value digests or expiring either target.
  const auto now = DataStore::now_ms();
  for (auto* client : clients) {
    if (!client) continue;
    for (auto& [key, watch] : client->watched_digests) {
      const auto old = live_.watch_state(key.first, key.second, now);
      const auto incoming = data_.watch_state(key.first, key.second, now);
      if (!old.exists && !incoming.exists) continue;
      if (watch.expired) {
        if (!incoming.exists) {
          watch.expired = false;
          continue;
        }
        if (incoming.expired) continue;
      } else if (!old.exists && incoming.expired) {
        watch.expired = true;
        continue;
      }
      client->watched_dirty = true;
    }
  }
  live_.exchange_dataset(data_);
  live_functions_.swap(functions_);
  live_.retire_dataset(data_, std::move(data_ticket_));
  retirement_.retire(functions_, std::move(function_ticket_));
  state_ = State::Published;
  return true;
}

void ReplicationLoadStage::discard() noexcept {
  check_owner();
  if (!data_ticket_) return;  // already published or discarded
  live_.retire_dataset(data_, std::move(data_ticket_));
  retirement_.retire(functions_, std::move(function_ticket_));
  state_ = State::Discarded;
}

const RdbLoadStats& ReplicationLoadStage::stats() const {
  check_owner();
  return stats_;
}

const RdbReplicationInfo& ReplicationLoadStage::replication() const {
  check_owner();
  return replication_;
}

std::optional<RdbStreamResult> ReplicationLoadStage::take_result() noexcept {
  check_owner();
  if (state_ != State::Published) return std::nullopt;
  state_ = State::Released;
  return std::move(result_);
}
}  // namespace peadb
