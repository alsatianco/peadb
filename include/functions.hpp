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

// Immutable after installation. The last registry retaining a library releases its Lua
// callback references on the owning thread, without recompiling any surviving callbacks.
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
  void check_owner() const noexcept;
  const std::thread::id owner_ = std::this_thread::get_id();
  std::map<std::string, std::shared_ptr<const FunctionLibrary>> libraries_;
  std::map<std::string, std::string> index_;
};

FunctionRegistry& live_functions();

}  // namespace peadb
