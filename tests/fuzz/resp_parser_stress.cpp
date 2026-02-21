#include "protocol.hpp"

#include <iostream>
#include <sstream>
#include <string>

int main() {
  std::ostringstream ss;
  ss << std::cin.rdbuf();
  std::string input = ss.str();
  (void)peadb::parse_one_command(input);
  return 0;
}
