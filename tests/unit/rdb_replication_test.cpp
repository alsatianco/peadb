// RDB history is published only with a successfully loaded dataset, never on a rejected
// header/checksum, structural preflight failure, or apply-time function error.
#include "rdb.hpp"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <unistd.h>

namespace {
int failures = 0;
void check(bool ok, const char* what) {
  if (!ok) {
    std::cerr << "FAIL: " << what << '\n';
    ++failures;
  }
}
std::string field(const std::string& s) {
  return std::string(1, static_cast<char>(s.size())) + s;
}
std::string fixture(bool function = false) {
  std::string data = "REDIS0011";
  for (const auto& [key, value] : std::vector<std::pair<std::string, std::string>>{
           {"RePl-Id", std::string(40, 'a')}, {"RePl-OfFsEt", "123"}, {"RePl-StReAm-Db", "5"}}) {
    data += '\xfa' + field(key) + field(value);
  }
  if (function) data += '\xf5' + field("library rejected by hook");
  data += '\xff';
  data.append(8, '\0');  // Redis accepts a file saved with checksums disabled.
  return data;
}
}  // namespace

int main() {
  char scratch[] = "/tmp/peadb-rdb-history-unit-XXXXXX";
  const char* dir = mkdtemp(scratch);
  if (!dir) return 1;
  const auto path = std::filesystem::path(dir) / "dump.rdb";
  std::string err;
  peadb::RdbReplicationInfo info;
  auto write = [&](const std::string& data) {
    std::ofstream out(path, std::ios::binary);
    out.write(data.data(), data.size());
  };
  write(fixture());
  check(peadb::rdb_load(path, err, nullptr, &info), "valid metadata fixture loads");
  check(info.valid() && info.stream_db == 5 && info.offset == 123 &&
            info.replid == std::string(40, 'a'),
        "case-insensitive AUX fields publish typed history");
  const auto good = info;
  int replacements = 0;
  auto reject = [&](const std::string& data, bool preflight) {
    write(data);
    info = good;
    const std::function<void()> replace = [&] {
      ++replacements;
    };
    check(!peadb::rdb_load(path, err, preflight ? replace : nullptr, &info), "bad load fails");
    check(info.replid.empty() && info.stream_db == -1 && info.offset == -1,
          "failure never publishes stale or partially parsed history");
  };
  reject("REDIS9999", true);
  auto bad_crc = fixture();
  bad_crc.back() = 1;
  reject(bad_crc, true);
  auto truncated = fixture();
  truncated.resize(truncated.size() - 9);  // AUX fields alone are not a complete RDB.
  truncated.append(8, '\0');
  reject(truncated, true);
  check(replacements == 0, "preflight failures precede dataset replacement");
  peadb::rdb_register_function_hooks(nullptr, [](const std::string&, std::string& error) {
    error = "test function application failure";
    return false;
  });
  reject(fixture(true), true);
  check(replacements == 1, "apply-time failure reaches replacement but cannot publish history");
  peadb::rdb_register_function_hooks(nullptr, nullptr);
  write(fixture());
  check(peadb::rdb_load(path, err, nullptr, &info) && info.valid(), "valid retry after failures");
  write(std::string("REDIS0011\xff") + std::string(8, '\0'));
  check(peadb::rdb_load(path, err, nullptr, &info) && !info.valid() && info.replid.empty(),
        "later metadata-free snapshot clears prior output");
  std::filesystem::remove_all(dir);
  return failures ? 1 : 0;
}
