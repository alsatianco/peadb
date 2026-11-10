#pragma once

#include "config.hpp"

#include <functional>

namespace peadb {

// Initialize listeners and client service before running the synchronous startup loader.
// A nonzero loader result stops the server; a loading shutdown exits through normal cleanup.
int run_server(const ServerConfig& config, const std::function<int()>& startup = {});

}  // namespace peadb
