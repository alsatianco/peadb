#pragma once

#include <string>

namespace peadb {

inline std::string wrongtype_error_reply() {
  return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
}

} // namespace peadb
