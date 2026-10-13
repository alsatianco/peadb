#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

// CRC16 used by Redis cluster to assign keys to hash slots (0..16383).
// When PEADB_NUM_SHARDS > 1 the listener distributes accepted fds to shards
// by computing crc16(key) % 16384 and then slot / (16384 / N_SHARDS).
// With the default PEADB_NUM_SHARDS=1 the shard layer is a no-op pass-through.

namespace peadb {

static constexpr int kNumHashSlots = 16384;

// Returns the Redis-compatible CRC16 of key bytes (used for slot assignment).
uint16_t crc16(const char* buf, std::size_t len);

// Returns the hash slot [0, 16383] for key, honouring {} hash-tag extraction.
int key_hash_slot(const char* key, std::size_t keylen);

// Returns which shard index owns slot for a given number of shards.
inline int slot_to_shard(int slot, int n_shards) {
  return slot / (kNumHashSlots / n_shards);
}

// ShardLoop encapsulates the per-shard event loop. When PEADB_NUM_SHARDS == 1
// this is a thin wrapper around the existing run_server() logic.
// When > 1, each ShardLoop runs in its own thread with an isolated DataStore
// slice (future: after P3-3 full global migration and P4-1 Entry variant).
class ShardLoop {
 public:
  explicit ShardLoop(int shard_id, int total_shards);
  ~ShardLoop();

  // Enqueue an accepted client fd for this shard to adopt.
  // Thread-safe: may be called from the listener thread.
  void enqueue_client(int fd);

  // Block until this shard's event loop exits.
  void join();

  int shard_id() const {
    return shard_id_;
  }

 private:
  int shard_id_;
  int total_shards_;
};

// Initialize all shards (called from main before accept loop).
// With n_shards == 1 this is a no-op (run_server handles everything itself).
void shard_init(int n_shards);

// Called by the listener for each newly accepted fd.
// Routes fd to the appropriate shard based on round-robin (initial connection
// cannot be slot-assigned until a command is parsed). Individual commands
// are then re-routed within the shard if needed.
void shard_dispatch_fd(int fd);

// Wait for all shard loops to finish (called at shutdown).
void shard_shutdown();

}  // namespace peadb
