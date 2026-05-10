#include "command.hpp"
#include "datastore.hpp"

#include <iostream>

int main() {
  peadb::SessionState session;
  session.db_index = 0;
  bool close_flag = false;

  // FLUSHALL is itself a write and increments the epoch
  peadb::handle_command({"FLUSHALL"}, session, close_flag);
  const auto base_epoch = peadb::store().mutation_epoch();

  // 5 write commands
  peadb::handle_command({"SET", "k1", "v1"}, session, close_flag);
  peadb::handle_command({"SET", "k2", "v2"}, session, close_flag);
  peadb::handle_command({"HSET", "h1", "f", "v"}, session, close_flag);
  peadb::handle_command({"LPUSH", "l1", "a"}, session, close_flag);
  peadb::handle_command({"SADD", "s1", "m"}, session, close_flag);

  auto epoch5 = peadb::store().mutation_epoch();
  if (epoch5 != base_epoch + 5) {
    std::cerr << "FAIL: mutation_epoch after 5 writes = " << epoch5
              << ", expected " << (base_epoch + 5) << "\n";
    return 1;
  }

  // Read-only command should not advance epoch
  peadb::handle_command({"GET", "k1"}, session, close_flag);
  auto epoch_after_get = peadb::store().mutation_epoch();
  if (epoch_after_get != base_epoch + 5) {
    std::cerr << "FAIL: mutation_epoch after GET = " << epoch_after_get
              << ", expected " << (base_epoch + 5) << "\n";
    return 1;
  }

  std::cout << "mutation_epoch_test passed\n";
  return 0;
}
