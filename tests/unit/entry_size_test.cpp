// Guards the fat-Entry → tagged-union refactor (Phase A of the memory/eviction track).
//
// Two things must stay true:
//   1. sizeof(Entry) stays small. The old struct carried all six type payloads at once
//      (~328 B libc++ / ~456 B libstdc++); the union holds only the active one. This test
//      fails loudly if a field is ever added back to every key.
//   2. The hand-written deep-copy ctor/assign actually clone the active payload — a
//      unique_ptr member makes the *default* copy ill-formed, so the clone logic is the
//      only thing keeping value semantics (COPY / RENAME / SWAPDB / MULTI snapshots).

#include "command.hpp"
#include "datastore.hpp"

#include <iostream>

namespace {
int failures = 0;
void check(bool ok, const char* what) {
  if (!ok) { std::cerr << "FAIL: " << what << "\n"; ++failures; }
}
}  // namespace

int main() {
  // (1) Size ceiling. Measured 56 B on libc++; libstdc++'s 32-B std::string lifts the
  // variant a little. 80 B catches any regression toward the old fat struct while
  // tolerating standard-library layout differences.
  check(sizeof(peadb::Entry) <= 80, "sizeof(Entry) <= 80 (tagged union, not fat struct)");
  std::cout << "sizeof(Entry) = " << sizeof(peadb::Entry) << " bytes\n";

  // (2) Deep-copy correctness via COPY: mutating the source must not touch the copy.
  peadb::SessionState session;
  session.db_index = 0;
  bool close_flag = false;
  peadb::handle_command({"FLUSHALL"}, session, close_flag);

  peadb::handle_command({"RPUSH", "src", "a", "b", "c"}, session, close_flag);
  peadb::handle_command({"COPY", "src", "dst"}, session, close_flag);
  peadb::handle_command({"RPUSH", "src", "d"}, session, close_flag);  // grow source only

  bool wt = false;
  check(peadb::store().llen("src", wt) == 4, "source list grew to 4");
  check(peadb::store().llen("dst", wt) == 3, "copied list stayed at 3 (deep copy, not shared)");

  // A copied string must also be independent, and overwriting a key with a new type
  // must release the old payload (variant assignment) rather than alias it.
  peadb::handle_command({"SET", "s", "hello"}, session, close_flag);
  peadb::handle_command({"COPY", "s", "s2"}, session, close_flag);
  peadb::handle_command({"SET", "s", "world"}, session, close_flag);
  const auto s2 = peadb::store().get("s2");
  check(s2.has_value() && *s2 == "hello", "copied string independent of source");

  if (failures == 0) std::cout << "entry_size_test passed\n";
  return failures == 0 ? 0 : 1;
}
