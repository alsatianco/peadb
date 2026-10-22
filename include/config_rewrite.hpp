#pragma once

#include <functional>
#include <string>
#include <sys/types.h>

namespace peadb {

struct RedisConfigSpec;

// Rewrite the original config file using canonical live values. Replication is supplied
// separately because a host is one argument even when it contains spaces. Returns false
// with errno preserved on read/publication failure; a missing original file is recreated.
bool rewrite_config_file(const std::string& path,
                         const std::function<std::string(const RedisConfigSpec&)>& value,
                         bool replica, const std::string& replica_host, int replica_port,
                         mode_t startup_umask);

}  // namespace peadb
