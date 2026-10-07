// The global operator new and operator delete, replaced to count the bytes the process holds, like
// Redis's zmalloc.c with HAVE_MALLOC_SIZE: each allocation adds the size the allocator gave it and
// each free subtracts the same size, read back with malloc_usable_size()/malloc_size(), so the
// count includes the allocator's rounding and never drifts (both sides ask the allocator, whatever
// size the caller passed to a sized delete). See include/zmalloc.hpp.
#include "zmalloc.hpp"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>

#if defined(__APPLE__)
#include <malloc/malloc.h>
#define PEADB_ALLOC_SIZE(p) malloc_size(p)
#elif defined(__linux__)
#include <malloc.h>
#define PEADB_ALLOC_SIZE(p) malloc_usable_size(p)
#else
#error "zmalloc.cpp needs malloc_usable_size() or malloc_size() on this platform"
#endif

namespace {

// Every thread allocates (the event loop, the lazyfree thread, the replication thread), so the
// count is atomic, a relaxed add like Redis's atomicIncr(used_memory, n). Constant-initialized, so
// it is ready for the first allocation of the static initializers.
std::atomic<std::int64_t> g_used_bytes{0};

inline void count_bytes(std::int64_t n) {
  g_used_bytes.fetch_add(n, std::memory_order_relaxed);
}

inline void* counted_alloc(std::size_t n) {
  if (n == 0) n = 1;
  void* p;
  while ((p = std::malloc(n)) == nullptr) {
    std::new_handler handler = std::get_new_handler();
    if (handler == nullptr) throw std::bad_alloc();
    handler();
  }
  count_bytes(static_cast<std::int64_t>(PEADB_ALLOC_SIZE(p)));
  return p;
}

inline void* counted_alloc_aligned(std::size_t n, std::align_val_t al) {
  if (n == 0) n = 1;
  std::size_t align = static_cast<std::size_t>(al);
  if (align < sizeof(void*)) align = sizeof(void*);
  void* p = nullptr;
  while (posix_memalign(&p, align, n) != 0) {
    std::new_handler handler = std::get_new_handler();
    if (handler == nullptr) throw std::bad_alloc();
    handler();
  }
  count_bytes(static_cast<std::int64_t>(PEADB_ALLOC_SIZE(p)));
  return p;
}

inline void counted_free(void* p) noexcept {
  if (p == nullptr) return;
  count_bytes(-static_cast<std::int64_t>(PEADB_ALLOC_SIZE(p)));
  std::free(p);
}

}  // namespace

namespace peadb {

std::size_t zmalloc_used_memory() {
  const std::int64_t total = g_used_bytes.load(std::memory_order_relaxed);
  return total > 0 ? static_cast<std::size_t>(total) : 0;
}

std::size_t zmalloc_string_bytes(const std::string& s) {
  static const std::size_t kInlineCapacity = std::string().capacity();
  if (s.capacity() <= kInlineCapacity) return 0;
  return PEADB_ALLOC_SIZE(const_cast<char*>(s.data()));
}

}  // namespace peadb

void* operator new(std::size_t n) {
  return counted_alloc(n);
}
void* operator new[](std::size_t n) {
  return counted_alloc(n);
}
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
  try {
    return counted_alloc(n);
  } catch (...) {
    return nullptr;
  }
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
  try {
    return counted_alloc(n);
  } catch (...) {
    return nullptr;
  }
}
void* operator new(std::size_t n, std::align_val_t al) {
  return counted_alloc_aligned(n, al);
}
void* operator new[](std::size_t n, std::align_val_t al) {
  return counted_alloc_aligned(n, al);
}
void* operator new(std::size_t n, std::align_val_t al, const std::nothrow_t&) noexcept {
  try {
    return counted_alloc_aligned(n, al);
  } catch (...) {
    return nullptr;
  }
}
void* operator new[](std::size_t n, std::align_val_t al, const std::nothrow_t&) noexcept {
  try {
    return counted_alloc_aligned(n, al);
  } catch (...) {
    return nullptr;
  }
}

void operator delete(void* p) noexcept {
  counted_free(p);
}
void operator delete[](void* p) noexcept {
  counted_free(p);
}
void operator delete(void* p, std::size_t) noexcept {
  counted_free(p);
}
void operator delete[](void* p, std::size_t) noexcept {
  counted_free(p);
}
void operator delete(void* p, const std::nothrow_t&) noexcept {
  counted_free(p);
}
void operator delete[](void* p, const std::nothrow_t&) noexcept {
  counted_free(p);
}
void operator delete(void* p, std::align_val_t) noexcept {
  counted_free(p);
}
void operator delete[](void* p, std::align_val_t) noexcept {
  counted_free(p);
}
void operator delete(void* p, std::size_t, std::align_val_t) noexcept {
  counted_free(p);
}
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept {
  counted_free(p);
}
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept {
  counted_free(p);
}
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept {
  counted_free(p);
}
