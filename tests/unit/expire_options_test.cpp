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

#define ASSERT(cond, msg) \
  do { if (!(cond)) { std::cerr << "FAIL: " << msg << "\n"; return 1; } } while(0)

static int test_expire_nx() {
  cmd({"FLUSHALL"});
  cmd({"SET", "k", "v"});

  // NX — set expiry only when none exists
  ASSERT(is_int(cmd({"EXPIRE", "k", "100", "NX"}), 1), "EXPIRE NX should set when no expiry");
  ASSERT(is_int(cmd({"EXPIRE", "k", "999", "NX"}), 0), "EXPIRE NX should not overwrite existing expiry");

  // TTL should still be ~100, not 999
  const auto ttl = cmd({"TTL", "k"});
  ASSERT(ttl != ":999\r\n", "EXPIRE NX must not overwrite — TTL changed unexpectedly");
  return 0;
}

static int test_expire_xx() {
  cmd({"FLUSHALL"});
  cmd({"SET", "k", "v"});

  // XX — set expiry only when one already exists
  ASSERT(is_int(cmd({"EXPIRE", "k", "100", "XX"}), 0), "EXPIRE XX should fail when no expiry exists");
  ASSERT(is_int(cmd({"TTL", "k"}), -1), "key should still have no expiry");

  cmd({"EXPIRE", "k", "100"});
  ASSERT(is_int(cmd({"EXPIRE", "k", "200", "XX"}), 1), "EXPIRE XX should succeed when expiry exists");
  return 0;
}

static int test_expire_gt() {
  cmd({"FLUSHALL"});
  cmd({"SET", "k", "v"});
  cmd({"EXPIRE", "k", "100"});

  ASSERT(is_int(cmd({"EXPIRE", "k", "50", "GT"}), 0), "EXPIRE GT should not decrease expiry");
  ASSERT(is_int(cmd({"EXPIRE", "k", "200", "GT"}), 1), "EXPIRE GT should increase expiry");
  return 0;
}

static int test_expire_lt() {
  cmd({"FLUSHALL"});
  cmd({"SET", "k", "v"});
  cmd({"EXPIRE", "k", "100"});

  ASSERT(is_int(cmd({"EXPIRE", "k", "200", "LT"}), 0), "EXPIRE LT should not increase expiry");
  ASSERT(is_int(cmd({"EXPIRE", "k", "50", "LT"}), 1), "EXPIRE LT should decrease expiry");
  return 0;
}

static int test_pexpire() {
  cmd({"FLUSHALL"});
  cmd({"SET", "k", "v"});

  ASSERT(is_int(cmd({"PEXPIRE", "k", "100000", "NX"}), 1), "PEXPIRE NX should set");
  ASSERT(is_int(cmd({"PEXPIRE", "k", "999000", "NX"}), 0), "PEXPIRE NX should not overwrite");
  return 0;
}

static int test_expiretime() {
  cmd({"FLUSHALL"});
  cmd({"SET", "k", "v"});

  // No expiry → -1
  ASSERT(is_int(cmd({"EXPIRETIME", "k"}), -1), "EXPIRETIME with no expiry should return -1");
  ASSERT(is_int(cmd({"PEXPIRETIME", "k"}), -1), "PEXPIRETIME with no expiry should return -1");

  // With expiry — result must be a positive absolute timestamp (> current seconds)
  cmd({"EXPIRE", "k", "100"});
  const auto et = cmd({"EXPIRETIME", "k"});
  ASSERT(et.size() > 0 && et[0] == ':', "EXPIRETIME should return an integer");
  long long ts = std::stoll(et.substr(1));
  ASSERT(ts > 1000000000LL, "EXPIRETIME should be a plausible Unix timestamp");

  const auto pet = cmd({"PEXPIRETIME", "k"});
  long long pts = std::stoll(pet.substr(1));
  ASSERT(pts > ts * 1000 - 1000, "PEXPIRETIME should be ~1000x EXPIRETIME in ms");
  return 0;
}

int main() {
  g_session.db_index = 0;

  if (test_expire_nx()) return 1;
  if (test_expire_xx()) return 1;
  if (test_expire_gt()) return 1;
  if (test_expire_lt()) return 1;
  if (test_pexpire())   return 1;
  if (test_expiretime()) return 1;

  std::cout << "expire_options_test passed\n";
  return 0;
}
