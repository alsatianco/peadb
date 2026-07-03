#pragma once

#include <string>

namespace peadb {

inline std::string wrongtype_error_reply() {
  return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
}

// Emit a stream consumer-group error: messages that carry their own error code
// (NOGROUP/BUSYGROUP) are emitted verbatim; everything else gets the generic "ERR " prefix.
inline std::string encode_stream_error(const std::string& err) {
  if (err.rfind("NOGROUP", 0) == 0 || err.rfind("BUSYGROUP", 0) == 0)
    return "-" + err + "\r\n";
  return "-ERR " + err + "\r\n";
}

} // namespace peadb
