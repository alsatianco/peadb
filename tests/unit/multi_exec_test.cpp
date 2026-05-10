#include "command.hpp"
#include "datastore.hpp"

#include <iostream>
#include <string>

static bool g_close = false;

static std::string cmd(peadb::SessionState& s, std::initializer_list<std::string> args) {
  return peadb::handle_command(std::vector<std::string>(args), s, g_close);
}

static peadb::SessionState make_session(int db = 0) {
  peadb::SessionState s;
  s.db_index = db;
  return s;
}

#define ASSERT(cond, msg) \
  do { if (!(cond)) { std::cerr << "FAIL: " << msg << "\n  got: " << (msg ## _got) << "\n"; return 1; } } while(0)
#define ASSERT_EQ(r, expected, label) \
  do { auto _r = (r); if (_r != (expected)) { \
    std::cerr << "FAIL: " << label << "\n  expected: " << (expected) << "\n  got:      " << _r << "\n"; \
    return 1; } } while(0)
#define ASSERT_HAS(r, sub, label) \
  do { auto _r = (r); if (_r.find(sub) == std::string::npos) { \
    std::cerr << "FAIL: " << label << "\n  expected to contain: " << (sub) << "\n  got: " << _r << "\n"; \
    return 1; } } while(0)

static int test_multi_exec_roundtrip() {
  auto s = make_session();
  cmd(s, {"FLUSHALL"});

  ASSERT_EQ(cmd(s, {"MULTI"}), "+OK\r\n", "MULTI should return OK");
  ASSERT_HAS(cmd(s, {"SET", "k", "queued"}), "QUEUED", "SET inside MULTI should be queued");
  ASSERT_HAS(cmd(s, {"GET", "k"}), "QUEUED", "GET inside MULTI should be queued");

  const auto exec_r = cmd(s, {"EXEC"});
  // EXEC returns array: [OK, bulk("queued")]
  ASSERT_HAS(exec_r, "+OK", "EXEC result must contain SET reply");
  ASSERT_HAS(exec_r, "queued", "EXEC result must contain GET reply");
  return 0;
}

static int test_discard() {
  auto s = make_session();
  cmd(s, {"FLUSHALL"});
  cmd(s, {"SET", "k", "original"});

  ASSERT_EQ(cmd(s, {"MULTI"}), "+OK\r\n", "MULTI");
  ASSERT_HAS(cmd(s, {"SET", "k", "modified"}), "QUEUED", "SET queued");
  ASSERT_EQ(cmd(s, {"DISCARD"}), "+OK\r\n", "DISCARD should return OK");

  // Key must still hold original value
  const auto get_r = cmd(s, {"GET", "k"});
  ASSERT_HAS(get_r, "original", "DISCARD: queued write must be rolled back");
  return 0;
}

static int test_exec_abort_on_syntax_error() {
  auto s = make_session();
  cmd(s, {"FLUSHALL"});

  ASSERT_EQ(cmd(s, {"MULTI"}), "+OK\r\n", "MULTI");
  // Queue a valid command and an unknown command
  ASSERT_HAS(cmd(s, {"SET", "k", "v"}), "QUEUED", "SET queued");
  const auto bad_r = cmd(s, {"NOTACOMMAND"});
  // Server should return an error immediately for an unknown command inside MULTI
  // and mark the transaction for abort
  const auto exec_r = cmd(s, {"EXEC"});
  // EXEC should either return EXECABORT error or nil (implementation-dependent)
  const bool aborted = exec_r.find("EXECABORT") != std::string::npos ||
                       exec_r == "*-1\r\n" ||
                       exec_r.find("-") == 0;
  if (!aborted) {
    std::cerr << "FAIL: EXEC after bad command should return EXECABORT or nil, got: " << exec_r << "\n";
    return 1;
  }
  return 0;
}

static int test_nested_multi_error() {
  auto s = make_session();
  cmd(s, {"FLUSHALL"});

  ASSERT_EQ(cmd(s, {"MULTI"}), "+OK\r\n", "MULTI");
  const auto nested = cmd(s, {"MULTI"});
  ASSERT_HAS(nested, "-", "nested MULTI should return an error");
  cmd(s, {"DISCARD"});
  return 0;
}

static int test_watch_abort() {
  auto s1 = make_session();
  auto s2 = make_session();
  cmd(s1, {"FLUSHALL"});
  cmd(s1, {"SET", "w", "0"});

  // Session 1 watches "w"
  ASSERT_EQ(cmd(s1, {"WATCH", "w"}), "+OK\r\n", "WATCH");
  ASSERT_EQ(cmd(s1, {"MULTI"}), "+OK\r\n", "MULTI after WATCH");
  ASSERT_HAS(cmd(s1, {"GET", "w"}), "QUEUED", "GET queued");

  // Session 2 modifies "w" before session 1 executes
  cmd(s2, {"SET", "w", "modified"});

  // EXEC on session 1 should return nil (abort) because "w" was touched
  const auto exec_r = cmd(s1, {"EXEC"});
  const bool aborted = exec_r == "*-1\r\n" ||
                       exec_r.find("EXECABORT") != std::string::npos;
  if (!aborted) {
    std::cerr << "FAIL: EXEC after WATCH+mutation should abort, got: " << exec_r << "\n";
    return 1;
  }
  return 0;
}

int main() {
  if (test_multi_exec_roundtrip())    return 1;
  if (test_discard())                 return 1;
  if (test_exec_abort_on_syntax_error()) return 1;
  if (test_nested_multi_error())      return 1;
  if (test_watch_abort())             return 1;

  std::cout << "multi_exec_test passed\n";
  return 0;
}
