#include "datastore.hpp"
#include "errors.hpp"

#include <iostream>

int main() {
  if (peadb::wrongtype_error_reply() != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n") {
    std::cerr << "wrongtype error text mismatch\n";
    return 1;
  }

  auto& s = peadb::store();
  s.flushall();
  if (s.is_wrongtype_for_string("missing")) {
    std::cerr << "missing key should not be wrongtype\n";
    return 1;
  }
  if (!s.set("k", "v", false, false, std::nullopt, false)) {
    std::cerr << "set failed\n";
    return 1;
  }
  if (s.is_wrongtype_for_string("k")) {
    std::cerr << "string key should not be wrongtype\n";
    return 1;
  }

  std::cout << "wrongtype_framework_test passed\n";
  return 0;
}
