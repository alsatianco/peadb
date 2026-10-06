#pragma once

// A chained hash table with a power-of-two number of buckets, like Redis's dict.c. The bucket of an
// element is `hash & (bucket_count - 1)`, so scan() can give Redis's SCAN guarantee across resizes:
// every element that is in the table for the whole of a scan is returned at least once, however much
// the table grows or shrinks between calls (std::unordered_map uses prime bucket counts, which cannot
// keep it; compat/delta.md item 32). The interface is the part of std::unordered_map /
// std::unordered_set that PeaDB uses, with the same rule for references (valid until the element is
// erased, also across a rehash). Differences from the std containers: an iterator is invalidated by
// a rehash (which only an insert, reserve() or shrink_if_sparse() can start) and also by swap(), a
// move or clear() of its table; erase(iterator) returns nothing, because finding the next element
// can mean walking many empty buckets (use `auto next = std::next(it); d.erase(it); it = next;` in a
// loop); clear() frees the bucket array (as Redis's dictEmpty() does). A rehash is done at once, not
// in steps (compat/delta.md item 13).
//
// Dict<Key, Mapped> is a map whose value_type is std::pair<const Key, Mapped>; Dict<Key, void> is a
// set whose value_type is Key (its iterators are read-only, like std::unordered_set's).
//
// DictStringHash is the hash for string keys: SipHash-1-2 with a key drawn at random when the
// process first hashes, like Redis's dictGenHashFunction() with the seed it sets at startup, so a
// client cannot choose keys that all land in one bucket (with a power-of-two table, keys whose hashes
// share their low bits collide at every table size).

#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

namespace peadb {

namespace dict_detail {

template <class Key, class Mapped>
struct Traits {
  using value_type = std::pair<const Key, Mapped>;
  static const Key& key_of(const value_type& v) { return v.first; }
};

template <class Key>
struct Traits<Key, void> {
  using value_type = Key;
  static const Key& key_of(const value_type& v) { return v; }
};

// Reverses the 64 bits of `v` (Redis's rev() in dict.c).
inline std::uint64_t reverse_bits(std::uint64_t v) {
  v = ((v >> 1) & 0x5555555555555555ULL) | ((v & 0x5555555555555555ULL) << 1);
  v = ((v >> 2) & 0x3333333333333333ULL) | ((v & 0x3333333333333333ULL) << 2);
  v = ((v >> 4) & 0x0F0F0F0F0F0F0F0FULL) | ((v & 0x0F0F0F0F0F0F0F0FULL) << 4);
  v = ((v >> 8) & 0x00FF00FF00FF00FFULL) | ((v & 0x00FF00FF00FF00FFULL) << 8);
  v = ((v >> 16) & 0x0000FFFF0000FFFFULL) | ((v & 0x0000FFFF0000FFFFULL) << 16);
  return (v >> 32) | (v << 32);
}

// The smallest power of two that is at least `n` (and at least 1).
inline std::size_t next_power_of_two(std::size_t n) {
  std::size_t p = 1;
  while (p < n) p <<= 1;
  return p;
}

inline std::uint64_t rotl64(std::uint64_t x, int b) { return (x << b) | (x >> (64 - b)); }

// Reads 8 bytes as a little-endian number, as SipHash defines its input words.
inline std::uint64_t load_le64(const unsigned char* p) {
  std::uint64_t v = 0;
  for (int i = 7; i >= 0; --i) v = (v << 8) | p[i];
  return v;
}

}  // namespace dict_detail

// SipHash-C-D (Aumasson and Bernstein) of `data` under the 128-bit key (k0, k1): C rounds per 8-byte
// word and D final rounds. Redis hashes keys with SipHash-1-2; SipHash-2-4 is the paper's standard
// form, with published test vectors (tests/unit/dict_test.cpp checks one).
template <int C, int D>
std::uint64_t siphash(const void* data, std::size_t len, std::uint64_t k0, std::uint64_t k1) {
  using dict_detail::rotl64;
  std::uint64_t v0 = k0 ^ 0x736f6d6570736575ULL;
  std::uint64_t v1 = k1 ^ 0x646f72616e646f6dULL;
  std::uint64_t v2 = k0 ^ 0x6c7967656e657261ULL;
  std::uint64_t v3 = k1 ^ 0x7465646279746573ULL;
  auto round = [&] {
    v0 += v1;
    v1 = rotl64(v1, 13);
    v1 ^= v0;
    v0 = rotl64(v0, 32);
    v2 += v3;
    v3 = rotl64(v3, 16);
    v3 ^= v2;
    v0 += v3;
    v3 = rotl64(v3, 21);
    v3 ^= v0;
    v2 += v1;
    v1 = rotl64(v1, 17);
    v1 ^= v2;
    v2 = rotl64(v2, 32);
  };
  const auto* p = static_cast<const unsigned char*>(data);
  const std::size_t whole = len - len % 8;
  for (std::size_t i = 0; i < whole; i += 8) {
    const std::uint64_t m = dict_detail::load_le64(p + i);
    v3 ^= m;
    for (int r = 0; r < C; ++r) round();
    v0 ^= m;
  }
  std::uint64_t b = static_cast<std::uint64_t>(len) << 56;
  for (std::size_t i = whole; i < len; ++i) b |= static_cast<std::uint64_t>(p[i]) << (8 * (i - whole));
  v3 ^= b;
  for (int r = 0; r < C; ++r) round();
  v0 ^= b;
  v2 ^= 0xff;
  for (int r = 0; r < D; ++r) round();
  return v0 ^ v1 ^ v2 ^ v3;
}

// The random SipHash key of this process, drawn once, on first use (a fork child keeps it).
struct DictHashKey {
  std::uint64_t k0;
  std::uint64_t k1;
};
inline const DictHashKey& dict_hash_key() {
  static const DictHashKey key = [] {
    std::random_device rd;
    auto draw = [&rd] { return (static_cast<std::uint64_t>(rd()) << 32) | rd(); };
    const std::uint64_t k0 = draw();
    return DictHashKey{k0, draw()};
  }();
  return key;
}

struct DictStringHash {
  std::size_t operator()(std::string_view s) const {
    const DictHashKey& key = dict_hash_key();
    return static_cast<std::size_t>(siphash<1, 2>(s.data(), s.size(), key.k0, key.k1));
  }
};

template <class Key, class Mapped, class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>>
class Dict {
  using Tr = dict_detail::Traits<Key, Mapped>;
  static constexpr bool kIsMap = !std::is_void_v<Mapped>;

 public:
  using key_type = Key;
  using mapped_type = Mapped;
  using value_type = typename Tr::value_type;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using hasher = Hash;
  using key_equal = KeyEqual;
  using reference = value_type&;
  using const_reference = const value_type&;

  // Redis's DICT_HT_INITIAL_SIZE: the first insert allocates 4 buckets.
  static constexpr size_type kInitialBuckets = 4;

 private:
  struct Node {
    Node* next = nullptr;
    std::size_t hash;
    value_type value;

    template <class... Args>
    explicit Node(std::size_t h, Args&&... args) : hash(h), value(std::forward<Args>(args)...) {}
  };

  template <bool Const>
  class Iter {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename Tr::value_type;
    using difference_type = std::ptrdiff_t;
    using reference = std::conditional_t<Const || !kIsMap, const value_type&, value_type&>;
    using pointer = std::conditional_t<Const || !kIsMap, const value_type*, value_type*>;

    Iter() = default;
    template <bool C = Const, std::enable_if_t<C, int> = 0>
    Iter(const Iter<false>& o) : d_(o.d_), n_(o.n_) {}  // iterator -> const_iterator

    reference operator*() const { return n_->value; }
    pointer operator->() const { return &n_->value; }
    Iter& operator++() {
      n_ = d_->next_node(n_);
      return *this;
    }
    Iter operator++(int) {
      Iter t = *this;
      ++*this;
      return t;
    }
    friend bool operator==(const Iter& a, const Iter& b) { return a.n_ == b.n_; }

   private:
    friend class Dict;
    template <bool>
    friend class Iter;
    Iter(const Dict* d, Node* n) : d_(d), n_(n) {}
    const Dict* d_ = nullptr;
    Node* n_ = nullptr;
  };

  // Walks one bucket's chain (std::unordered_map::local_iterator).
  template <bool Const>
  class LocalIter {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename Tr::value_type;
    using difference_type = std::ptrdiff_t;
    using reference = std::conditional_t<Const || !kIsMap, const value_type&, value_type&>;
    using pointer = std::conditional_t<Const || !kIsMap, const value_type*, value_type*>;

    LocalIter() = default;
    reference operator*() const { return n_->value; }
    pointer operator->() const { return &n_->value; }
    LocalIter& operator++() {
      n_ = n_->next;
      return *this;
    }
    LocalIter operator++(int) {
      LocalIter t = *this;
      n_ = n_->next;
      return t;
    }
    friend bool operator==(const LocalIter& a, const LocalIter& b) { return a.n_ == b.n_; }

   private:
    friend class Dict;
    explicit LocalIter(Node* n) : n_(n) {}
    Node* n_ = nullptr;
  };

 public:
  using iterator = std::conditional_t<kIsMap, Iter<false>, Iter<true>>;
  using const_iterator = Iter<true>;
  using local_iterator = std::conditional_t<kIsMap, LocalIter<false>, LocalIter<true>>;
  using const_local_iterator = LocalIter<true>;

  // An element taken out of the table by extract() (std's node handle, the parts PeaDB uses).
  class node_type {
   public:
    node_type() = default;
    node_type(node_type&& o) noexcept : n_(o.n_) { o.n_ = nullptr; }
    node_type& operator=(node_type&& o) noexcept {
      if (this != &o) {
        delete n_;
        n_ = o.n_;
        o.n_ = nullptr;
      }
      return *this;
    }
    node_type(const node_type&) = delete;
    node_type& operator=(const node_type&) = delete;
    ~node_type() { delete n_; }

    bool empty() const { return n_ == nullptr; }
    explicit operator bool() const { return n_ != nullptr; }
    const key_type& key() const { return Tr::key_of(n_->value); }
    template <bool M = kIsMap, std::enable_if_t<M, int> = 0>
    auto& mapped() const {
      return n_->value.second;
    }
    template <bool M = kIsMap, std::enable_if_t<!M, int> = 0>
    const value_type& value() const {
      return n_->value;
    }

   private:
    friend class Dict;
    explicit node_type(Node* n) : n_(n) {}
    Node* release() {
      Node* n = n_;
      n_ = nullptr;
      return n;
    }
    Node* n_ = nullptr;
  };

  struct insert_return_type {
    iterator position;
    bool inserted;
    node_type node;
  };

  Dict() = default;
  Dict(std::initializer_list<value_type> init) { insert(init.begin(), init.end()); }
  template <class InputIt>
  Dict(InputIt first, InputIt last) {
    insert(first, last);
  }

  Dict(const Dict& o) : hash_(o.hash_), eq_(o.eq_) { copy_from(o); }
  Dict(Dict&& o) noexcept
      : buckets_(std::move(o.buckets_)), nbuckets_(o.nbuckets_), size_(o.size_), first_(o.first_),
        hash_(std::move(o.hash_)), eq_(std::move(o.eq_)) {
    o.reset_empty();
  }
  Dict& operator=(const Dict& o) {
    if (this != &o) {
      Dict tmp(o);
      swap(tmp);
    }
    return *this;
  }
  Dict& operator=(Dict&& o) noexcept {
    if (this != &o) {
      free_nodes();
      buckets_ = std::move(o.buckets_);
      nbuckets_ = o.nbuckets_;
      size_ = o.size_;
      first_ = o.first_;
      hash_ = std::move(o.hash_);
      eq_ = std::move(o.eq_);
      o.reset_empty();
    }
    return *this;
  }
  ~Dict() { free_nodes(); }

  void swap(Dict& o) noexcept {
    using std::swap;
    swap(buckets_, o.buckets_);
    swap(nbuckets_, o.nbuckets_);
    swap(size_, o.size_);
    swap(first_, o.first_);
    swap(hash_, o.hash_);
    swap(eq_, o.eq_);
  }
  friend void swap(Dict& a, Dict& b) noexcept { a.swap(b); }

  // ── Iteration ──
  iterator begin() { return iterator(this, first_node()); }
  const_iterator begin() const { return const_iterator(this, first_node()); }
  const_iterator cbegin() const { return begin(); }
  iterator end() { return iterator(this, nullptr); }
  const_iterator end() const { return const_iterator(this, nullptr); }
  const_iterator cend() const { return end(); }

  // ── Size ──
  bool empty() const { return size_ == 0; }
  size_type size() const { return size_; }

  // ── Lookup ──
  iterator find(const key_type& k) { return iterator(this, find_node(k, hash_(k))); }
  const_iterator find(const key_type& k) const { return const_iterator(this, find_node(k, hash_(k))); }
  size_type count(const key_type& k) const { return find_node(k, hash_(k)) ? 1 : 0; }
  bool contains(const key_type& k) const { return find_node(k, hash_(k)) != nullptr; }

  template <bool M = kIsMap, std::enable_if_t<M, int> = 0>
  auto& at(const key_type& k) {
    Node* n = find_node(k, hash_(k));
    if (!n) throw std::out_of_range("Dict::at");
    return n->value.second;
  }
  template <bool M = kIsMap, std::enable_if_t<M, int> = 0>
  const auto& at(const key_type& k) const {
    const Node* n = find_node(k, hash_(k));
    if (!n) throw std::out_of_range("Dict::at");
    return n->value.second;
  }
  template <bool M = kIsMap, std::enable_if_t<M, int> = 0>
  auto& operator[](const key_type& k) {
    return try_emplace(k).first->second;
  }
  template <bool M = kIsMap, std::enable_if_t<M, int> = 0>
  auto& operator[](key_type&& k) {
    return try_emplace(std::move(k)).first->second;
  }

  // ── Insertion ──
  template <class... Args>
  std::pair<iterator, bool> emplace(Args&&... args) {
    // Like std: the element is built first, then its key looked up; a duplicate is dropped.
    std::unique_ptr<Node> owned(new Node(0, std::forward<Args>(args)...));
    const key_type& k = Tr::key_of(owned->value);
    const std::size_t h = hash_(k);
    if (Node* found = find_node(k, h)) return {iterator(this, found), false};
    owned->hash = h;
    return {iterator(this, link_new(std::move(owned))), true};
  }
  std::pair<iterator, bool> insert(const value_type& v) {
    const key_type& k = Tr::key_of(v);
    const std::size_t h = hash_(k);
    if (Node* found = find_node(k, h)) return {iterator(this, found), false};
    return {iterator(this, link_new(std::unique_ptr<Node>(new Node(h, v)))), true};
  }
  std::pair<iterator, bool> insert(value_type&& v) {
    const key_type& k = Tr::key_of(v);
    const std::size_t h = hash_(k);
    if (Node* found = find_node(k, h)) return {iterator(this, found), false};
    return {iterator(this, link_new(std::unique_ptr<Node>(new Node(h, std::move(v))))), true};
  }
  template <class InputIt>
  void insert(InputIt first, InputIt last) {
    for (; first != last; ++first) emplace(*first);
  }
  void insert(std::initializer_list<value_type> init) { insert(init.begin(), init.end()); }
  insert_return_type insert(node_type&& nh) {
    if (nh.empty()) return {end(), false, node_type()};
    const key_type& k = nh.key();
    const std::size_t h = hash_(k);
    if (Node* found = find_node(k, h)) return {iterator(this, found), false, std::move(nh)};
    nh.n_->hash = h;
    std::unique_ptr<Node> owned(nh.release());
    return {iterator(this, link_new(std::move(owned))), true, node_type()};
  }
  template <class K, class... Args, bool M = kIsMap, std::enable_if_t<M, int> = 0>
  std::pair<iterator, bool> try_emplace(K&& k, Args&&... args) {
    const std::size_t h = hash_(k);
    if (Node* found = find_node(k, h)) return {iterator(this, found), false};
    std::unique_ptr<Node> owned(new Node(h, std::piecewise_construct,
                                         std::forward_as_tuple(std::forward<K>(k)),
                                         std::forward_as_tuple(std::forward<Args>(args)...)));
    return {iterator(this, link_new(std::move(owned))), true};
  }
  template <class K, class M, bool IM = kIsMap, std::enable_if_t<IM, int> = 0>
  std::pair<iterator, bool> insert_or_assign(K&& k, M&& m) {
    auto r = try_emplace(std::forward<K>(k), std::forward<M>(m));
    if (!r.second) r.first->second = std::forward<M>(m);
    return r;
  }

  // ── Removal ──
  // O(1): unlike std, no iterator to the next element is returned (see the top of the file).
  void erase(const_iterator pos) {
    unlink(pos.n_);
    delete pos.n_;
  }
  template <bool M = kIsMap, std::enable_if_t<M, int> = 0>
  void erase(iterator pos) {
    erase(const_iterator(pos));
  }
  iterator erase(const_iterator first, const_iterator last) {
    while (first != last) {
      const const_iterator pos = first++;
      erase(pos);
    }
    return iterator(this, last.n_);
  }
  size_type erase(const key_type& k) {
    Node* n = find_node(k, hash_(k));
    if (!n) return 0;
    unlink(n);
    delete n;
    return 1;
  }
  node_type extract(const_iterator pos) {
    unlink(pos.n_);
    return node_type(pos.n_);
  }
  node_type extract(const key_type& k) {
    Node* n = find_node(k, hash_(k));
    if (!n) return node_type();
    unlink(n);
    return node_type(n);
  }
  // Deletes every element and frees the bucket array.
  void clear() {
    free_nodes();
    reset_empty();
  }

  // ── Buckets ──
  size_type bucket_count() const { return nbuckets_; }
  size_type bucket_size(size_type b) const {
    size_type n = 0;
    for (const Node* p = buckets_[b]; p; p = p->next) ++n;
    return n;
  }
  size_type bucket(const key_type& k) const { return hash_(k) & mask(); }
  local_iterator begin(size_type b) { return local_iterator(buckets_[b]); }
  const_local_iterator begin(size_type b) const { return const_local_iterator(buckets_[b]); }
  local_iterator end(size_type) { return local_iterator(nullptr); }
  const_local_iterator end(size_type) const { return const_local_iterator(nullptr); }
  float load_factor() const {
    return nbuckets_ == 0 ? 0.0f : static_cast<float>(size_) / static_cast<float>(nbuckets_);
  }
  float max_load_factor() const { return 1.0f; }
  // Makes room for `n` elements without a rehash (never shrinks).
  void reserve(size_type n) {
    if (n > nbuckets_) rehash_to(dict_detail::next_power_of_two(n < kInitialBuckets ? kInitialBuckets : n));
  }
  // Redis's htNeedsResize() and dictResize(): when less than 10% of the buckets are used, shrink the
  // table to the smallest power of two that holds every element (at least 4 buckets). Returns true
  // when it resized, which invalidates iterators like any rehash.
  bool shrink_if_sparse() {
    if (nbuckets_ <= kInitialBuckets || size_ * 100 / nbuckets_ >= 10) return false;
    rehash_to(dict_detail::next_power_of_two(size_ < kInitialBuckets ? kInitialBuckets : size_));
    return true;
  }

  // Redis's dictScan() for a table that is not rehashing: calls fn(element) for each element of the
  // bucket that `cursor` names (cursor & (bucket_count - 1)) and returns the next cursor, 0 when the
  // scan is over. The cursor counts with its bits reversed, so a scan that started before the table
  // grew or shrank still returns every element that was present for the whole scan (some may come
  // twice). fn must not change the table.
  template <class Fn>
  std::uint64_t scan(std::uint64_t cursor, Fn&& fn) const {
    if (size_ == 0) return 0;
    const std::uint64_t m = mask();
    for (const Node* n = buckets_[cursor & m]; n;) {
      const Node* next = n->next;
      fn(static_cast<const value_type&>(n->value));
      n = next;
    }
    cursor |= ~m;
    cursor = dict_detail::reverse_bits(cursor);
    ++cursor;
    return dict_detail::reverse_bits(cursor);
  }

 private:
  static_assert(sizeof(std::size_t) == sizeof(std::uint64_t), "Dict's scan cursor needs a 64-bit size_t");

  std::size_t mask() const { return nbuckets_ - 1; }

  Node* find_node(const key_type& k, std::size_t h) const {
    if (size_ == 0) return nullptr;
    for (Node* n = buckets_[h & mask()]; n; n = n->next) {
      if (n->hash == h && eq_(Tr::key_of(n->value), k)) return n;
    }
    return nullptr;
  }

  // The first element, starting at first_ (no bucket before first_ holds one).
  Node* first_node() const {
    if (size_ == 0) return nullptr;
    std::size_t b = first_;
    while (!buckets_[b]) ++b;
    first_ = b;
    return buckets_[b];
  }

  // The element after `n` in iteration order: the rest of its chain, then the next non-empty bucket.
  Node* next_node(const Node* n) const {
    if (n->next) return n->next;
    for (std::size_t b = (n->hash & mask()) + 1; b < nbuckets_; ++b) {
      if (buckets_[b]) return buckets_[b];
    }
    return nullptr;
  }

  // Links a new element (its key is not in the table), first growing the table like Redis's
  // _dictExpandIfNeeded(): to 4 buckets on the first insert, and to twice the size once there are as
  // many elements as buckets. A failed allocation leaves the table unchanged (and frees the node).
  Node* link_new(std::unique_ptr<Node> owned) {
    if (nbuckets_ == 0) {
      rehash_to(kInitialBuckets);
    } else if (size_ >= nbuckets_) {
      rehash_to(nbuckets_ * 2);
    }
    Node* n = owned.release();
    const std::size_t b = n->hash & mask();
    n->next = buckets_[b];
    buckets_[b] = n;
    ++size_;
    if (b < first_) first_ = b;
    return n;
  }

  void unlink(Node* n) {
    Node** pp = &buckets_[n->hash & mask()];
    while (*pp != n) pp = &(*pp)->next;
    *pp = n->next;
    --size_;
  }

  // Moves every element into a new array of `n` buckets (a power of two, at least size_ / 1).
  void rehash_to(std::size_t n) {
    std::unique_ptr<Node*[]> nb(new Node*[n]());
    const std::size_t m = n - 1;
    for (std::size_t b = 0; b < nbuckets_; ++b) {
      Node* p = buckets_[b];
      while (p) {
        Node* next = p->next;
        const std::size_t i = p->hash & m;
        p->next = nb[i];
        nb[i] = p;
        p = next;
      }
    }
    buckets_ = std::move(nb);
    nbuckets_ = n;
    first_ = 0;
  }

  // Copies `o`'s elements chain by chain, keeping its layout. On a failed allocation the elements
  // copied so far are freed and the exception goes on.
  void copy_from(const Dict& o) {
    if (o.size_ == 0) return;
    buckets_.reset(new Node*[o.nbuckets_]());
    nbuckets_ = o.nbuckets_;
    first_ = o.first_;
    try {
      for (std::size_t b = 0; b < o.nbuckets_; ++b) {
        Node** tail = &buckets_[b];
        for (const Node* p = o.buckets_[b]; p; p = p->next) {
          *tail = new Node(p->hash, p->value);
          tail = &(*tail)->next;
          ++size_;
        }
      }
    } catch (...) {
      free_nodes();
      reset_empty();
      throw;
    }
  }

  void free_nodes() {
    if (size_ == 0) return;
    for (std::size_t b = 0; b < nbuckets_; ++b) {
      Node* p = buckets_[b];
      while (p) {
        Node* next = p->next;
        delete p;
        p = next;
      }
    }
  }

  void reset_empty() {
    buckets_.reset();
    nbuckets_ = 0;
    size_ = 0;
    first_ = 0;
  }

  std::unique_ptr<Node*[]> buckets_;
  std::size_t nbuckets_ = 0;
  std::size_t size_ = 0;
  mutable std::size_t first_ = 0;  // no bucket before this one holds an element (a hint for begin())
  [[no_unique_address]] Hash hash_;
  [[no_unique_address]] KeyEqual eq_;
};

template <class Key, class Mapped, class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>>
using DictMap = Dict<Key, Mapped, Hash, KeyEqual>;
template <class Key, class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>>
using DictSet = Dict<Key, void, Hash, KeyEqual>;

}  // namespace peadb
