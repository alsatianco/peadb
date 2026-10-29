#include "crash.hpp"

#include "logger.hpp"
#include "peadb_version.hpp"
#include "signal_trace.hpp"

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <dlfcn.h>
#include <exception>
#include <initializer_list>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>

namespace peadb {
namespace {
std::atomic<bool> enabled{true};
static_assert(decltype(enabled)::is_always_lock_free);
// Written before installing handlers; never changed while a handler can run.
std::uintptr_t executable_base = 0;
// Only the main stack is registered. Handler-time TLS access can allocate on
// macOS when a worker first touches this module; use immutable process state.
std::uintptr_t stack_low = 0, stack_high = 0;
alignas(16) char alternate_stack[64 * 1024];

void number_line(const char* prefix, std::uintptr_t value, unsigned base) {
  char line[192];
  unsigned size = 0;
  while (*prefix && size < 128) line[size++] = *prefix++;
  char digits[2 * sizeof(value) + 1];
  unsigned count = 0;
  do {
    digits[count++] = "0123456789abcdef"[value % base];
    value /= base;
  } while (value && count < sizeof(digits));
  while (count) line[size++] = digits[--count];
  line[size] = 0;
  log_from_signal_handler(LogLevel::Warn, line);
}

__attribute__((no_sanitize("address", "undefined"))) void crash_handler(int sig, siginfo_t* info,
                                                                        void* context) {
  // POSIX permits SA_RESETHAND to leave SIGILL unchanged (macOS does so).
  // Reset explicitly before diagnostics so a second fault cannot recurse.
  struct sigaction action {};
  action.sa_handler = SIG_DFL;
  sigemptyset(&action.sa_mask);
  sigaction(sig, &action, nullptr);
  if (enabled.load(std::memory_order_relaxed)) {
    log_from_signal_handler(LogLevel::Warn, "=== PEADB BUG REPORT START ===");
    number_line("PeaDB " PEADB_VERSION " crashed by signal: ", sig, 10);
    if (info && (sig == SIGSEGV || sig == SIGBUS))
      number_line("Accessing address: 0x", reinterpret_cast<std::uintptr_t>(info->si_addr), 16);
    number_line("Executable load address: 0x", executable_base, 16);
    log_from_signal_handler(LogLevel::Warn, "------ STACK TRACE ------");
    // Preserve the real instruction even if a damaged frame chain faults again.
    const auto instruction = capture_signal_trace(context, 0, 0);
    if (instruction.count != 0)
      number_line("frame 0: 0x", reinterpret_cast<std::uintptr_t>(instruction.frames[0]), 16);
    const auto trace = capture_signal_trace(context, stack_low, stack_high);
    for (int i = 1; i < trace.count; ++i)
      number_line("frame: 0x", reinterpret_cast<std::uintptr_t>(trace.frames[i]), 16);
    log_from_signal_handler(LogLevel::Warn, "=== PEADB BUG REPORT END ===");
  }
  // The default is restored above. Unblock before re-raising so
  // neither a pending delivery nor a worker can allow the faulting code to resume.
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, sig);
  sigprocmask(SIG_UNBLOCK, &mask, nullptr);
  raise(sig);
  _exit(128 + sig);  // Only if the platform could not deliver the signal.
}
}  // namespace

bool crash_log_enabled() {
  return enabled.load();
}

void configure_crash_log(bool value) {
  enabled.store(value);
}

void install_crash_handlers() {
  const auto self = pthread_self();
#if defined(__APPLE__)
  stack_high = reinterpret_cast<std::uintptr_t>(pthread_get_stackaddr_np(self));
  stack_low = stack_high - pthread_get_stacksize_np(self);
#elif defined(__linux__)
  pthread_attr_t attr;
  if (pthread_getattr_np(self, &attr) == 0) {
    void* address = nullptr;
    std::size_t size = 0;
    if (pthread_attr_getstack(&attr, &address, &size) == 0) {
      stack_low = reinterpret_cast<std::uintptr_t>(address);
      stack_high = stack_low + size;
    }
    pthread_attr_destroy(&attr);
  }
#endif
  Dl_info image{};
  if (dladdr(reinterpret_cast<void*>(&install_crash_handlers), &image))
    executable_base = reinterpret_cast<std::uintptr_t>(image.dli_fbase);
  stack_t stack{};
  stack.ss_sp = alternate_stack;
  stack.ss_size = sizeof(alternate_stack);
  sigaltstack(&stack, nullptr);  // Normal-stack fallback if the alternate stack is unavailable.
  struct sigaction action {};
  action.sa_sigaction = crash_handler;
  sigfillset(&action.sa_mask);
  action.sa_flags = SA_SIGINFO | SA_RESETHAND | SA_ONSTACK;
  for (int sig : {SIGSEGV, SIGABRT, SIGBUS, SIGFPE, SIGILL}) sigaction(sig, &action, nullptr);
  std::set_terminate([] { std::abort(); });
}

__attribute__((noinline, no_sanitize("address", "undefined"))) void debug_segfault() {
  void* page = mmap(nullptr, 4096, PROT_READ, MAP_PRIVATE | MAP_ANON, -1, 0);
  if (page == MAP_FAILED) {
    raise(SIGSEGV);
    return;
  }
  *static_cast<volatile char*>(page) = 'x';
  munmap(page, 4096);  // Unreachable on a conforming platform.
}
}  // namespace peadb
