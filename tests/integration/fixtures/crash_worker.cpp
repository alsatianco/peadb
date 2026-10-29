#include "crash.hpp"
#include "logger.hpp"

#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <dlfcn.h>
#include <pthread.h>
#include <sys/resource.h>
#include <unistd.h>

namespace {
volatile sig_atomic_t reporting = 0;
#ifdef __APPLE__
void memory_log(unsigned, std::uintptr_t, std::uintptr_t, std::uintptr_t, std::uintptr_t,
                unsigned) {
  if (reporting) {
    constexpr char text[] = "ALLOC\n";
    write(STDERR_FILENO, text, sizeof(text) - 1);
  }
}
#endif
void* worker(void*) {
  // Deliberately do not access any state from crash.cpp before this signal.
  raise(SIGABRT);
  return nullptr;
}
}  // namespace

namespace peadb {
void log_from_signal_handler(LogLevel, const char* text) {
  reporting = 1;
  std::size_t size = 0;
  while (text[size]) ++size;
  write(STDOUT_FILENO, text, size);
  write(STDOUT_FILENO, "\n", 1);
}
}  // namespace peadb

int main() {
  rlimit limit{0, 0};
  if (setrlimit(RLIMIT_CORE, &limit)) return 2;
  peadb::install_crash_handlers();
#ifdef __APPLE__
  // Unlike malloc interposition, malloc_logger also sees dyld's internal TLS
  // calloc. A canary proves that the hook is active before testing the handler.
  using Logger = decltype(&memory_log);
  auto slot = static_cast<Logger*>(dlsym(RTLD_DEFAULT, "malloc_logger"));
  if (!slot) return 3;
  *slot = memory_log;
  reporting = 1;
  void* canary = malloc(300);
  asm volatile("" : : "r"(canary) : "memory");
  free(canary);
  reporting = 0;
  constexpr char end[] = "CANARY END\n";
  write(STDERR_FILENO, end, sizeof(end) - 1);
#endif
  pthread_t thread;
  if (pthread_create(&thread, nullptr, worker, nullptr)) return 4;
  pthread_join(thread, nullptr);
  return 5;  // The handler must terminate the process with SIGABRT.
}
