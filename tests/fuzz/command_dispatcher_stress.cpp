#include "command.hpp"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

int main() {
  std::ostringstream ss;
  ss << std::cin.rdbuf();
  std::string input = ss.str();

  std::vector<std::string> args;
  std::string token;
  std::istringstream iss(input);
  while (iss >> token) {
    args.push_back(token);
  }

  peadb::SessionState session {};
  bool should_close = false;
  (void)peadb::handle_command(args, session, should_close);
  return 0;
}
