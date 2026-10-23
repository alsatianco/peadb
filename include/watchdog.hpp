#pragma once

namespace peadb {

// The event loop has a fixed 100 ms housekeeping tick (hz is not implemented).
// Like Redis, clamp an enabled watchdog to at least two housekeeping ticks.
int watchdog_period();
bool configure_watchdog(int period);
// Owns the main-thread context and reporter for one run_server() invocation.
class EventLoopWatchdog {
 public:
  EventLoopWatchdog();
  ~EventLoopWatchdog();
  EventLoopWatchdog(const EventLoopWatchdog&) = delete;
  EventLoopWatchdog& operator=(const EventLoopWatchdog&) = delete;
};
void watchdog_rearm();

}  // namespace peadb
