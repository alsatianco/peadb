#include "watchdog.hpp"

#include "logger.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <execinfo.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/time.h>
#include <stdexcept>
#include <thread>
#include <sys/ucontext.h>
#include <unistd.h>

#if defined(__APPLE__) && defined(__arm64e__)
#include <ptrauth.h>
#endif

namespace peadb {
namespace {
int period_ms = 0;
bool active = false;
std::atomic<pthread_t> main_thread{};
std::atomic<std::uintptr_t> stack_low{0}, stack_high{0};
static_assert(decltype(main_thread)::is_always_lock_free);
static_assert(decltype(stack_low)::is_always_lock_free);
volatile sig_atomic_t report_fd = -1;
int reader_fd = -1;
std::thread reporter;

struct Trace {
  void* frames[48];
  int count;
};
static_assert(sizeof(Trace) <= 512);  // POSIX minimum PIPE_BUF: one atomic nonblocking write

bool schedule(int period) {
  struct itimerval timer {};
  timer.it_value.tv_sec = period / 1000;
  timer.it_value.tv_usec = (period % 1000) * 1000;
  return ::setitimer(ITIMER_REAL, &timer, nullptr) == 0;
}

// No unwinder, loader, allocator, C++ logger or lock in this handler. Frame
// pointers are retained in our build; bound every read to the main stack. A
// library without frame pointers may truncate the chain; the interrupted PC
// is always retained. Symbolization happens on a separate thread.
__attribute__((no_sanitize("address", "undefined"))) void capture(int, siginfo_t*, void* context) {
  const int saved_errno = errno;
  // ITIMER_REAL is process directed. Workers can inherit an unblocked SIGALRM;
  // forward it to the event-loop thread rather than reporting their stacks.
  const pthread_t target = main_thread.load(std::memory_order_relaxed);
  if (!pthread_equal(pthread_self(), target)) {
    pthread_kill(target, SIGALRM);
    errno = saved_errno;
    return;
  }
  auto* uc = static_cast<ucontext_t*>(context);
  std::uintptr_t pc = 0, fp = 0, sp = 0;
#if defined(__APPLE__) && defined(__aarch64__)
  pc = uc->uc_mcontext->__ss.__pc;
  fp = uc->uc_mcontext->__ss.__fp;
  sp = uc->uc_mcontext->__ss.__sp;
#elif defined(__APPLE__) && defined(__x86_64__)
  pc = uc->uc_mcontext->__ss.__rip;
  fp = uc->uc_mcontext->__ss.__rbp;
  sp = uc->uc_mcontext->__ss.__rsp;
#elif defined(__linux__) && defined(__aarch64__)
  pc = uc->uc_mcontext.pc;
  fp = uc->uc_mcontext.regs[29];
  sp = uc->uc_mcontext.sp;
#elif defined(__linux__) && defined(__x86_64__)
  pc = uc->uc_mcontext.gregs[REG_RIP];
  fp = uc->uc_mcontext.gregs[REG_RBP];
  sp = uc->uc_mcontext.gregs[REG_RSP];
#endif
  Trace trace{};
  trace.count = 0;
  if (pc) trace.frames[trace.count++] = reinterpret_cast<void*>(pc);
  const auto low = stack_low.load(std::memory_order_relaxed);
  const auto high = stack_high.load(std::memory_order_relaxed);
  while (trace.count < 48 && fp >= sp && fp >= low && high >= 2 * sizeof(std::uintptr_t) &&
         fp <= high - 2 * sizeof(std::uintptr_t) && fp % alignof(std::uintptr_t) == 0) {
    auto* frame = reinterpret_cast<const volatile std::uintptr_t*>(fp);
    const std::uintptr_t next = frame[0];
    auto* address = reinterpret_cast<void*>(frame[1]);
#if defined(__APPLE__) && defined(__arm64e__)
    address = ptrauth_strip(address, ptrauth_key_return_address);
#endif
    if (address) trace.frames[trace.count++] = address;
    if (next <= fp) break;
    fp = next;
  }
  // Keep the raw diagnostic usable even if symbolization waits on a loader or
  // allocator lock held by the stalled main thread.
  log_from_signal_handler(LogLevel::Warn, "--- WATCHDOG TIMER EXPIRED ---");
  for (int i = 0; i < trace.count; ++i) {
    char line[2 + 2 * sizeof(std::uintptr_t) + 1];
    line[0] = '0';
    line[1] = 'x';
    auto address = reinterpret_cast<std::uintptr_t>(trace.frames[i]);
    for (unsigned j = 0; j < 2 * sizeof(address); ++j) {
      line[sizeof(line) - 2 - j] = "0123456789abcdef"[address & 15];
      address >>= 4;
    }
    line[sizeof(line) - 1] = 0;
    log_from_signal_handler(LogLevel::Warn, line);
  }
  const ssize_t written = ::write(report_fd, &trace, sizeof(trace));
  (void)written;  // A full pipe drops supplemental symbols; never block the event loop.
  errno = saved_errno;
}

void report() {
  for (;;) {
    Trace trace;
    std::size_t done = 0;
    while (done < sizeof(trace)) {
      const ssize_t n =
          ::read(reader_fd, reinterpret_cast<char*>(&trace) + done, sizeof(trace) - done);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) return;
      done += static_cast<std::size_t>(n);
    }
    if (trace.count == 0) return;
    char** symbols = backtrace_symbols(trace.frames, trace.count);
    if (symbols) {
      try {
        std::string text = "WATCHDOG STACK TRACE (interrupted event loop)";
        for (int i = 0; i < trace.count; ++i) text += "\n" + std::string(symbols[i]);
        log(LogLevel::Warn, text);
      } catch (...) {
        // Memory pressure must not turn supplemental diagnostics into a crash;
        // the raw signal-safe addresses have already been written.
      }
      std::free(symbols);
    }
  }
}

bool start_reporter() {
  if (report_fd >= 0) return true;
  int fds[2];
  if (::pipe(fds) != 0) return false;
  if (::fcntl(fds[0], F_SETFD, FD_CLOEXEC) == -1 || ::fcntl(fds[1], F_SETFD, FD_CLOEXEC) == -1 ||
      ::fcntl(fds[1], F_SETFL, O_NONBLOCK) == -1) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  reader_fd = fds[0];
  report_fd = fds[1];
  // The reporter itself must never receive or forward the process timer.
  sigset_t mask, old;
  sigemptyset(&mask);
  sigaddset(&mask, SIGALRM);
  pthread_sigmask(SIG_BLOCK, &mask, &old);
  try {
    reporter = std::thread(report);
  } catch (...) {
    ::close(reader_fd);
    ::close(report_fd);
    reader_fd = -1;
    report_fd = -1;
    pthread_sigmask(SIG_SETMASK, &old, nullptr);
    return false;
  }
  pthread_sigmask(SIG_SETMASK, &old, nullptr);
  return true;
}
}  // namespace

int watchdog_period() {
  return period_ms;
}

bool configure_watchdog(int period) {
  period = period == 0 ? 0 : std::max(period, 200);
  if (!active) {
    period_ms = period;
    return true;
  }
  if (period != 0 && !start_reporter()) return false;
  struct sigaction action {};
  sigemptyset(&action.sa_mask);
  action.sa_flags = SA_SIGINFO | SA_RESTART;
  action.sa_sigaction = capture;
  if (period == 0) action.sa_handler = SIG_IGN;
  // Disable first; SIG_IGN also discards pending alarms as in Redis.
  if (!schedule(0) || sigaction(SIGALRM, &action, nullptr) != 0 || !schedule(period)) return false;
  period_ms = period;
  return true;
}

EventLoopWatchdog::EventLoopWatchdog() {
  const pthread_t self = pthread_self();
  main_thread = self;
#if defined(__APPLE__)
  const auto high = reinterpret_cast<std::uintptr_t>(pthread_get_stackaddr_np(self));
  stack_high = high;
  stack_low = high - pthread_get_stacksize_np(self);
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
  active = true;
  if (!configure_watchdog(period_ms))
    throw std::runtime_error("Unable to initialize event-loop watchdog");
}

EventLoopWatchdog::~EventLoopWatchdog() {
  configure_watchdog(0);
  active = false;
  if (reporter.joinable()) {
    // Disable O_NONBLOCK outside the handler to reliably enqueue the sentinel.
    ::fcntl(report_fd, F_SETFL, 0);
    Trace stop{};
    ssize_t n;
    do {
      n = ::write(report_fd, &stop, sizeof(stop));
    } while (n < 0 && errno == EINTR);
    reporter.join();
    ::close(report_fd);
    ::close(reader_fd);
    report_fd = -1;
    reader_fd = -1;
  }
}

void watchdog_rearm() {
  if (period_ms != 0) schedule(period_ms);
}
}  // namespace peadb
