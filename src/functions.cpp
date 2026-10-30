#include "functions.hpp"
#include "lua_engine.hpp"

#include <algorithm>
#include <cctype>
#include <exception>
#include <utility>

namespace peadb {
namespace {
std::string lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}
std::string upper(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return s;
}
bool valid_lib_name(const std::string& s) {
  if (s.empty()) return false;
  for (unsigned char c : s)
    if (!(std::isalnum(c) || c == '_')) return false;
  return true;
}

// Parse the `#!<engine> name=<name> [meta ...]` shebang. On success fills engine/name and body
// (= source after the shebang line). On failure sets `err` to the exact Redis message.
bool parse_function_shebang(const std::string& payload, std::string& engine, std::string& name,
                            std::string& body, std::string& err) {
  engine.clear();
  name.clear();
  body.clear();
  if (payload.rfind("#!", 0) != 0) {
    err = "Missing library metadata";
    return false;
  }
  const auto nl = payload.find('\n');
  if (nl == std::string::npos) {
    err = "Invalid library metadata";
    return false;
  }
  const std::string line = payload.substr(0, nl);
  body = payload.substr(nl + 1);  // Redis: the code starts at the '\n' (shebang line excluded)
  std::vector<std::string> parts;
  for (std::size_t i = 0; i < line.size();) {
    while (i < line.size() && std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    const std::size_t s = i;
    while (i < line.size() && !std::isspace(static_cast<unsigned char>(line[i]))) ++i;
    if (i > s) parts.push_back(line.substr(s, i - s));
  }
  if (parts.empty()) {
    err = "Invalid library metadata";
    return false;
  }
  engine = parts[0].rfind("#!", 0) == 0 ? parts[0].substr(2) : parts[0];
  bool have_name = false;
  for (std::size_t i = 1; i < parts.size(); ++i) {
    const std::string& tok = parts[i];
    if (tok.size() >= 5 && lower(tok.substr(0, 5)) == "name=") {
      if (have_name) {
        err = "Invalid metadata value, name argument was given multiple times";
        return false;
      }
      std::string v = tok.substr(5);
      if (v.size() >= 2 && v.front() == '"' && v.back() == '"') v = v.substr(1, v.size() - 2);
      name = v;
      have_name = true;
    } else {
      err = "Invalid metadata value given: " + tok;
      return false;
    }
  }
  if (!have_name) {
    err = "Library name was not given";
    return false;
  }
  return true;
}

}  // namespace

FunctionLibrary::~FunctionLibrary() {
  if (owner != std::this_thread::get_id()) std::terminate();
  for (const auto& f : functions) lua_engine_unref_function(f.callback_ref);
}

void FunctionRegistry::check_owner() const noexcept {
  if (owner_ != std::this_thread::get_id()) std::terminate();
}
FunctionRegistry::FunctionRegistry(const FunctionRegistry& other) {
  other.check_owner();
  libraries_ = other.libraries_;
  index_ = other.index_;
}
FunctionRegistry::~FunctionRegistry() {
  check_owner();
}
void FunctionRegistry::clear() noexcept {
  check_owner();
  index_.clear();
  libraries_.clear();
}
void FunctionRegistry::swap(FunctionRegistry& other) noexcept {
  check_owner();
  other.check_owner();
  libraries_.swap(other.libraries_);
  index_.swap(other.index_);
}
bool FunctionRegistry::erase(const std::string& name) {
  check_owner();
  auto it = libraries_.find(lower(name));
  if (it == libraries_.end()) return false;
  for (const auto& f : it->second->functions) index_.erase(f.canonical_name);
  libraries_.erase(it);
  return true;
}

bool FunctionRegistry::load(const std::string& code, bool replace, std::string& out_name,
                            std::string& err) {
  check_owner();
  std::string engine, name, body;
  if (!parse_function_shebang(code, engine, name, body, err)) return false;
  if (!valid_lib_name(name)) {
    err =
        "Library names can only contain letters, numbers, or underscores(_) and must be at least "
        "one character long";
    return false;
  }
  if (upper(engine) != "LUA") {
    err = "Engine '" + engine + "' not found";
    return false;
  }
  const auto lc = lower(name);
  auto existing = libraries_.find(lc);
  if (existing != libraries_.end() && !replace) {
    err = "Library '" + name + "' already exists";
    return false;
  }

  struct Compiled {
    std::vector<LuaRegisteredFunction> functions;
    ~Compiled() {
      for (const auto& f : functions) lua_engine_unref_function(f.callback_ref);
    }
  } compiled;
  if (!lua_engine_load_library(body, compiled.functions, err)) return false;
  auto lib = std::make_shared<FunctionLibrary>();
  lib->name = name;
  lib->engine = "LUA";
  lib->code = code;
  lib->functions.reserve(compiled.functions.size());
  std::map<std::string, std::string> new_index;
  for (auto& f : compiled.functions) {
    auto canonical = lower(f.name);
    auto idx = index_.find(canonical);
    if (idx != index_.end() && idx->second != lc) {
      err = "Function " + f.name + " already exists";
      return false;
    }
    new_index.emplace(canonical, lc);
    lib->functions.push_back(
        {f.name, f.description, f.flags, f.callback_ref, std::move(canonical)});
    f.callback_ref = -1;  // ownership transferred only after the insertion succeeded
  }
  std::map<std::string, std::shared_ptr<const FunctionLibrary>> new_library;
  new_library.emplace(lc, std::move(lib));
  out_name = name;

  // Everything fallible, including the returned name and map nodes, is prepared above.
  // std::map node merges with the same allocator do not allocate or rehash.
  if (existing != libraries_.end()) {
    for (const auto& f : existing->second->functions) index_.erase(f.canonical_name);
    libraries_.erase(existing);
  }
  index_.merge(new_index);
  libraries_.merge(new_library);
  return true;
}

bool FunctionRegistry::restore(const std::vector<std::string>& codes, const std::string& policy,
                               std::string& err) {
  check_owner();
  if (policy != "APPEND" && policy != "REPLACE" && policy != "FLUSH") {
    err = "Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.";
    return false;
  }
  if (policy == "APPEND") {
    for (const auto& code : codes) {
      std::string engine, name, body, ignored;
      if (parse_function_shebang(code, engine, name, body, ignored) &&
          libraries_.count(lower(name))) {
        err = "Library " + name + " already exists";
        return false;
      }
    }
  }
  FunctionRegistry staging(*this);
  if (policy == "FLUSH") staging.clear();
  std::string name;
  for (const auto& code : codes)
    if (!staging.load(code, policy == "REPLACE", name, err)) return false;
  swap(staging);
  return true;
}

FunctionRegistry& live_functions() {
  static FunctionRegistry registry;
  return registry;
}
}  // namespace peadb
