#pragma once

namespace peadb {
void install_crash_handlers();
bool crash_log_enabled();
void configure_crash_log(bool enabled);
// Redis's DEBUG SEGFAULT: write to an owned read-only page to generate a real
// synchronous fault (SIGBUS on macOS, SIGSEGV on Linux).
void debug_segfault();
}  // namespace peadb
