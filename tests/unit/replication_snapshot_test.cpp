#include "replication_snapshot.hpp"

#include <cstdlib>
#include <iostream>
#include <random>

using peadb::ReplicationSnapshot;
using namespace std::string_literals;

static void check(bool ok, const char* message) {
  if (!ok) {
    std::cerr << message << '\n';
    std::exit(1);
  }
}

int main() {
  const std::string marker = "0123456789abcdef0123456789abcdef01234567";
  std::string data;
  for (int i = 0; i < 1000; ++i) {
    for (int j = 0; j < 256; ++j) data += static_cast<char>(j);
    data += marker.substr(0, 39) + "!";
  }
  data += "\xff\0\0\0\0\0\0\0\0"s;
  const std::string command = "*1\r\n$4\r\nPING\r\n";
  std::mt19937 rng(725);
  for (bool eof : {false, true}) {
    // Include every trailer split, one-byte input, maximum read, and seeded partitions.
    for (std::size_t fragment = 1; fragment <= 43; ++fragment) {
      ReplicationSnapshot frame;
      check(frame.start(eof ? "$EOF:" + marker : "$" + std::to_string(data.size())), "header");
      check(!frame.start("$0"), "no reinitialization");
      std::string output, ahead;
      std::size_t max_write = 0;
      auto sink = [&](std::string_view bytes) {
        max_write = std::max(max_write, bytes.size());
        output.append(bytes);
        return true;
      };
      const std::string wire = data + (eof ? marker : "") + command;
      std::size_t offset = 0;
      while (!frame.done()) {
        const auto size = fragment == 42 ? ReplicationSnapshot::kChunk
                         : fragment == 43 ? 1 + rng() % ReplicationSnapshot::kChunk
                                          : fragment;
        const auto count = std::min(size, wire.size() - offset);
        check(count != 0 && frame.feed(std::string_view(wire).substr(offset, count), sink, ahead),
              "fragment feed");
        offset += count;
        check(frame.pending() <= 39, "bounded rolling suffix");
      }
      ahead += wire.substr(offset);
      check(output == data && ahead == command, "exact snapshot and read-ahead");
      check(frame.received() == static_cast<std::int64_t>(data.size()) &&
                frame.size() == frame.received(),
            "exact byte counts");
      check(max_write <= ReplicationSnapshot::kChunk, "bounded sink span");
      check(!frame.feed("more", sink, ahead), "no feed after boundary");
    }
  }
  for (const auto& header : {"", "$", "-1", "$-1", "$+1", "$12x", "$ 1", "$1\r", "$EOF:",
                             "$9223372036854775808", "$EOF:short"}) {
    ReplicationSnapshot frame;
    check(!frame.start(header), "malformed header refused");
  }
  for (int size : {39, 41}) {
    ReplicationSnapshot frame;
    check(!frame.start("$EOF:" + std::string(size, 'a')), "exact marker width");
  }
  {
    ReplicationSnapshot empty;
    check(empty.start("$0") && empty.done() && empty.received() == 0, "empty bulk");
  }
  auto sink = [](std::string_view) { return true; };
  for (bool eof : {false, true}) {
    ReplicationSnapshot frame;
    check(frame.start(eof ? "$EOF:" + marker : "$100"), "short header");
    std::string ahead;
    check(frame.feed("partial", sink, ahead) && !frame.done(), "short input never complete");
    check(frame.feed(std::string(40, '!'), sink, ahead) && !frame.done(), "wrong trailer");
  }
  {
    ReplicationSnapshot frame;
    std::string ahead;
    check(frame.start("$EOF:" + marker), "error header");
    auto fail = [](std::string_view) { return false; };
    check(!frame.feed(std::string(100, 'a'), fail, ahead) && !frame.done() &&
              frame.received() == 0,
          "sink failure propagates without counting unapplied bytes");
    check(!frame.feed(marker, sink, ahead), "failed attempt cannot resume");
  }
  {
    ReplicationSnapshot frame;
    std::string ahead;
    check(frame.start("$9223372036854775807"), "largest length header");
    check(!frame.feed(std::string(ReplicationSnapshot::kChunk + 1, 'a'), sink, ahead),
          "oversized feed refused");
  }
  std::cout << "PASS replication snapshot framing\n";
}
