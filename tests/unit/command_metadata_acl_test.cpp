#include "command_metadata.generated.hpp"

#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

// Assert the invariants of the *committed* include/command_metadata.generated.hpp table
// (the artifact that actually ships), not of a fresh run of
// scripts/ci/generate_command_metadata.py. The generator has its own self-check, but that
// only runs when someone remembers to regenerate; this test runs on every `ctest` so a bad
// edit to the derivation logic that is not followed by a regeneration cannot ship silently.
//
// Redis's implicit ACL category rule lives in setImplicitACLCategories()
// (~/.cache/peadb/redis-7.2.5/src/server.c:2958-2976); the full category enum is
// ACLCommandCategories (~/.cache/peadb/redis-7.2.5/src/acl.c:64-87).

namespace {

std::set<std::string> splitCats(const std::string& cats) {
  std::set<std::string> out;
  std::istringstream stream(cats);
  std::string cat;
  while (stream >> cat) out.insert(cat);
  return out;
}

const peadb::CommandMetaEntry* findEntry(const std::vector<peadb::CommandMetaEntry>& table,
                                          const std::string& full_name) {
  for (const auto& e : table) {
    if (full_name == e.full_name) return &e;
  }
  return nullptr;
}

}  // namespace

int main() {
  const auto& table = peadb::command_metadata();
  int failures = 0;

  if (table.empty()) {
    std::cerr << "command_metadata() returned an empty table\n";
    return 1;
  }

  // Invariant 1: no entry has an empty acl_cats. Before the implicit-categories rule was
  // added, 132 of 392 entries were empty.
  for (const auto& e : table) {
    if (std::string(e.acl_cats).empty()) {
      std::cerr << "invariant 1 violated: entry '" << e.full_name << "' has empty acl_cats\n";
      ++failures;
    }
  }

  // Invariant 2: all 21 of Redis's ACL categories appear somewhere in the table, in
  // ACLCommandCategories order. Before the implicit-categories rule, only 15 appeared --
  // read, write, fast, slow, blocking, pubsub were all absent.
  const std::vector<std::string> kExpectedCategories = {
      "keyspace", "read",     "write",       "set",         "sortedset",  "list",
      "hash",     "string",   "bitmap",      "hyperloglog", "geo",        "stream",
      "pubsub",   "admin",    "fast",        "slow",        "blocking",   "dangerous",
      "connection", "transaction", "scripting",
  };
  std::set<std::string> seen_categories;
  for (const auto& e : table) {
    for (const auto& cat : splitCats(e.acl_cats)) seen_categories.insert(cat);
  }
  for (const auto& cat : kExpectedCategories) {
    if (seen_categories.find(cat) == seen_categories.end()) {
      std::cerr << "invariant 2 violated: category '" << cat
                << "' does not appear on any entry in the table\n";
      ++failures;
    }
  }

  // Invariant 3: @fast and @slow partition the table -- Redis's rule is "if it's not @fast
  // it is @slow", so every entry must carry exactly one of the two, never both, never
  // neither. This is the strongest single invariant: it catches a broken rule immediately.
  for (const auto& e : table) {
    const auto cats = splitCats(e.acl_cats);
    const bool has_fast = cats.count("fast") != 0;
    const bool has_slow = cats.count("slow") != 0;
    if (has_fast == has_slow) {
      std::cerr << "invariant 3 violated: entry '" << e.full_name << "' has acl_cats=\""
                << e.acl_cats << "\" -- must carry exactly one of fast/slow, has "
                << (has_fast ? "both" : "neither") << "\n";
      ++failures;
    }
  }

  // Invariant 4: the scripting exclusion holds -- no entry carries both scripting and read.
  // Redis excludes scripting commands from the read category (server.c:2962-2963).
  for (const auto& e : table) {
    const auto cats = splitCats(e.acl_cats);
    if (cats.count("scripting") != 0 && cats.count("read") != 0) {
      std::cerr << "invariant 4 violated: entry '" << e.full_name << "' has acl_cats=\""
                << e.acl_cats << "\" -- carries both scripting and read\n";
      ++failures;
    }
  }

  // Invariant 5: concrete spot-checks so a wholesale rewrite of the derivation logic is
  // caught even if it happens to preserve invariants 1-4.
  struct SpotCheck {
    const char* full_name;
    std::vector<std::string> must_have;
    std::vector<std::string> must_not_have;
  };
  const std::vector<SpotCheck> spot_checks = {
      {"get", {"read", "fast"}, {"write"}},
      {"set", {"write", "slow"}, {"fast"}},
      {"debug", {"admin", "dangerous"}, {}},
  };
  for (const auto& check : spot_checks) {
    const auto* entry = findEntry(table, check.full_name);
    if (entry == nullptr) {
      std::cerr << "invariant 5 violated: spot-check entry '" << check.full_name
                << "' is missing from the table\n";
      ++failures;
      continue;
    }
    const auto cats = splitCats(entry->acl_cats);
    for (const auto& want : check.must_have) {
      if (cats.count(want) == 0) {
        std::cerr << "invariant 5 violated: entry '" << entry->full_name << "' has acl_cats=\""
                  << entry->acl_cats << "\" -- expected to carry '" << want << "'\n";
        ++failures;
      }
    }
    for (const auto& unwanted : check.must_not_have) {
      if (cats.count(unwanted) != 0) {
        std::cerr << "invariant 5 violated: entry '" << entry->full_name << "' has acl_cats=\""
                  << entry->acl_cats << "\" -- must not carry '" << unwanted << "'\n";
        ++failures;
      }
    }
  }

  if (failures != 0) {
    std::cerr << "command_metadata_acl_test failed: " << failures << " invariant violation(s) over "
              << table.size() << " entries\n";
    return 1;
  }

  std::cout << "command_metadata_acl_test passed: " << table.size() << " entries, "
            << kExpectedCategories.size() << " categories verified\n";
  return 0;
}
