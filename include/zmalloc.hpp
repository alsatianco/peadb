#pragma once

#include <cstddef>
#include <string>

namespace peadb {

// Bytes held by every live C++ heap allocation of the process, as the allocator sizes them
// (malloc_usable_size() on Linux, malloc_size() on macOS). src/zmalloc.cpp replaces the global
// operator new and operator delete to keep this count, like Redis's zmalloc_used_memory(): it is
// INFO's used_memory and what maxmemory is checked against. O(1). Memory the C code allocates with
// malloc() itself (the Lua interpreter, as in Redis) is not counted.
std::size_t zmalloc_used_memory();

// The bytes a std::string's heap buffer adds to zmalloc_used_memory() (what freeing it would give
// back): the allocator's size of the block, which can be larger than capacity() (macOS may hand
// back a bigger freed region). 0 when the string keeps its text inside the object. Redis's
// sdsAllocSize()/zmalloc_size().
std::size_t zmalloc_string_bytes(const std::string& s);

}  // namespace peadb
