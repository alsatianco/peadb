#include "protocol.hpp"

#include <cstdint>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  const std::string input(reinterpret_cast<const char*>(data), size);
  (void)peadb::parse_one_command(input);
  return 0;
}
