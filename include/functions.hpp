#pragma once

#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace peadb {

struct FunctionEntry {
  std::string name;
  std::string description;
  int flags = 0;
  int callback_ref = -1;
  std::string canonical_name;
};

// Immutable while retained outside a retirement queue. The last registry retaining a
// library releases its Lua callbacks on the owner thread without recompiling survivors.
struct FunctionLibrary {
  FunctionLibrary() = default;
  FunctionLibrary(const FunctionLibrary&) = delete;
  FunctionLibrary& operator=(const FunctionLibrary&) = delete;
  std::string name;
  std::string engine;
  std::string code;
  std::vector<FunctionEntry> functions;
  const std::thread::id owner = std::this_thread::get_id();
  ~FunctionLibrary();
};

// All operations, including copies/destruction, belong to the Lua-owning server thread.
// Contexts must be cleared/destroyed before the engine shuts down. A copy shares immutable
// library records, so it preserves closure identities and can stage APPEND/REPLACE safely.
class FunctionRegistry {
 public:
  FunctionRegistry() = default;
  FunctionRegistry(const FunctionRegistry& other);
  FunctionRegistry& operator=(const FunctionRegistry&) = delete;
  ~FunctionRegistry();

  bool load(const std::string& code, bool replace, std::string& name, std::string& err);
  bool restore(const std::vector<std::string>& codes, const std::string& policy, std::string& err);
  bool erase(const std::string& name);
  void clear() noexcept;
  void swap(FunctionRegistry& other) noexcept;

  const auto& libraries() const {
    check_owner();
    return libraries_;
  }
  const auto& index() const {
    check_owner();
    return index_;
  }

 private:
  friend class FunctionRetirementQueue;
  void check_owner() const noexcept;
  const std::thread::id owner_ = std::this_thread::get_id();
  std::map<std::string, std::shared_ptr<const FunctionLibrary>> libraries_;
  std::map<std::string, std::string> index_;
};

// Internal retirement for a detached old/failed staging registry. All operations and
// destruction belong to the Lua owner thread, before engine shutdown. Preparation may
// throw without changing registries. Submission only swaps/links prepared owners; it
// performs no allocation, free, Lua call or registry traversal. The caller chooses when
// to drain, outside Lua/command execution; this does not enable replication publication.
class FunctionRetirementQueue {
 public:
  class Ticket {
   public:
    ~Ticket();

   private:
    friend class FunctionRetirementQueue;
    explicit Ticket(FunctionRetirementQueue* owner) : owner_(owner) {
    }
    FunctionRetirementQueue* owner_;
    FunctionRegistry registry_;
    std::unique_ptr<Ticket> next_;
  };

  FunctionRetirementQueue() = default;
  FunctionRetirementQueue(const FunctionRetirementQueue&) = delete;
  FunctionRetirementQueue& operator=(const FunctionRetirementQueue&) = delete;
  ~FunctionRetirementQueue();

  std::unique_ptr<Ticket> prepare();
  void retire(FunctionRegistry& detached, std::unique_ptr<Ticket> ticket) noexcept;
  // One unit erases an index node, releases one callback and its metadata, releases a
  // library owner, or destroys an empty ticket. Shared libraries are never modified.
  // Returns units used, <= budget; zero does no work. This is a work bound, not a wall
  // clock/GC bound. External strong library owners remain responsible for final release;
  // weak observers must not access a library after its last external owner is dropped.
  std::size_t drain(std::size_t budget) noexcept;
  std::size_t pending() const noexcept;

 private:
  void check_owner() const noexcept;
  const std::thread::id owner_ = std::this_thread::get_id();
  std::unique_ptr<Ticket> head_;
  Ticket* tail_ = nullptr;
  std::size_t pending_ = 0;
};

FunctionRegistry& live_functions();

}  // namespace peadb
