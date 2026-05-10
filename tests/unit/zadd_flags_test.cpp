#include "command.hpp"
#include "datastore.hpp"

#include <iostream>
#include <string>

static peadb::SessionState g_session;
static bool g_close = false;

static std::string cmd(std::initializer_list<std::string> args) {
  return peadb::handle_command(std::vector<std::string>(args), g_session, g_close);
}

static bool is_int(const std::string& r, long long v) {
  return r == ":" + std::to_string(v) + "\r\n";
}

// Extract bulk string value from a $N\r\n...\r\n reply
static std::string bulk_val(const std::string& r) {
  const auto nl = r.find("\r\n");
  if (nl == std::string::npos || r[0] != '$') return "";
  return r.substr(nl + 2, r.size() - nl - 4);
}

#define ASSERT(cond, msg) \
  do { if (!(cond)) { std::cerr << "FAIL: " << msg << "\n"; return 1; } } while(0)

static int test_zadd_nx() {
  cmd({"FLUSHALL"});
  // NX — only add, never update
  ASSERT(is_int(cmd({"ZADD", "z", "NX", "1", "a"}), 1), "ZADD NX: new member should be added");
  ASSERT(is_int(cmd({"ZADD", "z", "NX", "99", "a"}), 0), "ZADD NX: existing member must not be updated");
  // Score must still be 1
  ASSERT(bulk_val(cmd({"ZSCORE", "z", "a"})) == "1", "ZADD NX: score must remain unchanged");
  return 0;
}

static int test_zadd_xx() {
  cmd({"FLUSHALL"});
  // XX — only update, never add
  ASSERT(is_int(cmd({"ZADD", "z", "XX", "5", "a"}), 0), "ZADD XX: non-existent member must not be added");
  // Verify member was not added
  const auto score = cmd({"ZSCORE", "z", "a"});
  ASSERT(score == "$-1\r\n" || score.find("nil") != std::string::npos || bulk_val(score).empty(),
         "ZADD XX: member must not exist");

  cmd({"ZADD", "z", "1", "a"});
  ASSERT(is_int(cmd({"ZADD", "z", "XX", "9", "a"}), 0), "ZADD XX: update returns 0 (count of added)");
  ASSERT(bulk_val(cmd({"ZSCORE", "z", "a"})) == "9", "ZADD XX: score must be updated");
  return 0;
}

static int test_zadd_gt() {
  cmd({"FLUSHALL"});
  cmd({"ZADD", "z", "5", "a"});
  // GT — only update when new score > current
  ASSERT(is_int(cmd({"ZADD", "z", "GT", "3", "a"}), 0), "ZADD GT: must not decrease score");
  ASSERT(bulk_val(cmd({"ZSCORE", "z", "a"})) == "5", "ZADD GT: score must remain 5");
  ASSERT(is_int(cmd({"ZADD", "z", "GT", "10", "a"}), 0), "ZADD GT: update returns 0 added");
  ASSERT(bulk_val(cmd({"ZSCORE", "z", "a"})) == "10", "ZADD GT: score must become 10");
  return 0;
}

static int test_zadd_lt() {
  cmd({"FLUSHALL"});
  cmd({"ZADD", "z", "5", "a"});
  // LT — only update when new score < current
  ASSERT(is_int(cmd({"ZADD", "z", "LT", "9", "a"}), 0), "ZADD LT: must not increase score");
  ASSERT(bulk_val(cmd({"ZSCORE", "z", "a"})) == "5", "ZADD LT: score must remain 5");
  ASSERT(is_int(cmd({"ZADD", "z", "LT", "2", "a"}), 0), "ZADD LT: update returns 0 added");
  ASSERT(bulk_val(cmd({"ZSCORE", "z", "a"})) == "2", "ZADD LT: score must become 2");
  return 0;
}

static int test_zadd_incr() {
  cmd({"FLUSHALL"});
  cmd({"ZADD", "z", "5", "a"});
  // INCR — behaves like ZINCRBY, returns new score as bulk string
  const auto r = cmd({"ZADD", "z", "INCR", "3", "a"});
  ASSERT(bulk_val(r) == "8", "ZADD INCR: must return new score 8");
  ASSERT(bulk_val(cmd({"ZSCORE", "z", "a"})) == "8", "ZADD INCR: score must be 8 in store");
  return 0;
}

static int test_zadd_ch() {
  cmd({"FLUSHALL"});
  cmd({"ZADD", "z", "1", "a"});
  // CH — return count of changed (added + updated), not just added
  ASSERT(is_int(cmd({"ZADD", "z", "CH", "2", "a", "3", "b"}), 2),
         "ZADD CH: 1 updated + 1 added = 2 changed");
  return 0;
}

int main() {
  g_session.db_index = 0;

  if (test_zadd_nx()) return 1;
  if (test_zadd_xx()) return 1;
  if (test_zadd_gt()) return 1;
  if (test_zadd_lt()) return 1;
  if (test_zadd_incr()) return 1;
  if (test_zadd_ch())   return 1;

  std::cout << "zadd_flags_test passed\n";
  return 0;
}
