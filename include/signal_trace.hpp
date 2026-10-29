#pragma once

#include <cstdint>
#include <sys/ucontext.h>

#if defined(__APPLE__) && defined(__arm64e__)
#include <ptrauth.h>
#endif

namespace peadb {
struct SignalTrace {
  void* frames[48];
  int count;
};

// Interrupted PC plus a bounded frame chain. No allocator, unwinder or locks.
// An interrupted stack outside the registered bounds retains only its PC.
__attribute__((no_sanitize("address", "undefined"))) inline SignalTrace capture_signal_trace(
    void* context, std::uintptr_t low, std::uintptr_t high) {
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
  SignalTrace trace{};
  if (pc) trace.frames[trace.count++] = reinterpret_cast<void*>(pc);
  if (sp < low || sp >= high) return trace;
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
  return trace;
}
}  // namespace peadb
