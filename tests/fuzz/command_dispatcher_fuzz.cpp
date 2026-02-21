#include "command.hpp"

#include <cstdint>
#include <string>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  std::vector<std::string> args;
  std::string current;

  for (size_t i = 0; i < size; ++i) {
    const char c = static_cast<char>(data[i]);
    if (c == ' ') {
      if (!current.empty()) {
        args.push_back(current);
        current.clear();
      }
    } else {
      current.push_back(c);
    }
  }
  if (!current.empty()) {
    args.push_back(current);
  }

  peadb::SessionState session {};
  bool should_close = false;
  (void)peadb::handle_command(args, session, should_close);
  return 0;
}
