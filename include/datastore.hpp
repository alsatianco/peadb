#pragma once

#include <cstdint>
#include <optional>
#include <deque>
#include <unordered_set>
#include <map>
#include <tuple>
#include <string>
#include <atomic>
#include <filesystem>
#include <unordered_map>
#include <vector>
#include <variant>
#include <memory>
#include <utility>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace peadb {

enum class ValueType {
  String,
  Hash,
  List,
  Set,
  ZSet,
  Stream,
};

// maxmemory-policy. `allkeys-*` sample the whole keyspace; `volatile-*` only keys
// with a TTL. The victim is chosen by approximate LRU (oldest access), LFU (lowest
// access frequency), TTL (soonest to expire), or at random.
enum class MaxmemoryPolicy {
  NoEviction,
  AllkeysLru,
  AllkeysLfu,
  AllkeysRandom,
  VolatileLru,
  VolatileLfu,
  VolatileRandom,
  VolatileTtl,
};

// Per-key value. Only the ACTIVE type's payload is allocated: the `payload` variant
// holds the inline string (String type) or a unique_ptr to the active collection, so a
// small string key no longer pays for six empty containers. `expire_at_ms`, `lru`, and
// `string_force_raw` stay inline (read regardless of type). The variant alternatives are
// ordered to match ValueType, so `type()` == `payload.index()`.
//
// Accessors mirror the old field names (`.hash()`, `.list()`, `.set()`, `.zset()`,
// `.zord()`, `.str()`, `.stream_*()`) and are the *only* way the rest of the code touches
// payloads. The mutable accessors materialize the active payload (get-or-create), which
// also replaces the old manual type-reset logic.
struct Entry {
  struct StreamGroup {
    std::string last_delivered_id = "0-0";
    std::unordered_map<std::string, std::string> pending_to_consumer;     // id -> consumer
    std::unordered_map<std::string, std::size_t> pending_per_consumer;    // consumer -> count
    std::unordered_map<std::string, std::int64_t> pending_delivery_ms;    // id -> last delivery time (ms)
    std::unordered_map<std::string, std::int64_t> pending_delivery_count; // id -> delivery count
  };

  using HashT = std::unordered_map<std::string, std::string>;
  using ListT = std::deque<std::string>;
  using SetT  = std::unordered_set<std::string>;
  struct ZSetT {
    std::unordered_map<std::string, double> by_member;            // member → score (O(1) lookup)
    std::map<std::pair<double, std::string>, bool> ordered;       // {score,member} sorted index
  };
  struct StreamT {
    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> entries;
    std::uint64_t last_ms = 0;
    std::uint64_t last_seq = 0;
    std::uint64_t entries_added = 0;      // total ever XADDed (XINFO entries-added)
    std::string max_deleted_id = "0-0";   // highest id ever XDELed (XINFO max-deleted-entry-id)
    std::unordered_map<std::string, StreamGroup> groups;
  };
  using Payload = std::variant<std::string,
                               std::unique_ptr<HashT>,
                               std::unique_ptr<ListT>,
                               std::unique_ptr<SetT>,
                               std::unique_ptr<ZSetT>,
                               std::unique_ptr<StreamT>>;

  std::optional<std::int64_t> expire_at_ms;
  std::uint32_t lru = 0;             // eviction metadata (LRU clock / LFU counter), Phase B
  bool string_force_raw = false;
  // Sticky encoding conversion: once a collection is observed past its listpack/intset
  // thresholds it reports the big encoding forever, like Redis's one-way conversions
  // (set on OBJECT ENCODING computation — an approximation until real backends exist).
  bool encoding_big = false;
  // Sets only: permanently off the intset encoding (a non-integer member was added, or
  // the set grew past set-max-intset-entries). Maintained at write time in sadd().
  bool encoding_no_intset = false;
  Payload payload{std::string{}};

  ValueType type() const { return static_cast<ValueType>(payload.index()); }

  // ── Mutable accessors (materialize the active payload, destroying any prior one) ──
  std::string& str() {
    if (payload.index() != 0) payload.emplace<std::string>();
    return std::get<std::string>(payload);
  }
  HashT& hash()   { if (payload.index() != 1) payload = std::make_unique<HashT>();   return *std::get<std::unique_ptr<HashT>>(payload); }
  ListT& list()   { if (payload.index() != 2) payload = std::make_unique<ListT>();   return *std::get<std::unique_ptr<ListT>>(payload); }
  SetT&  set()    { if (payload.index() != 3) payload = std::make_unique<SetT>();    return *std::get<std::unique_ptr<SetT>>(payload); }
  ZSetT& zset_payload() { if (payload.index() != 4) payload = std::make_unique<ZSetT>(); return *std::get<std::unique_ptr<ZSetT>>(payload); }
  StreamT& stream() { if (payload.index() != 5) payload = std::make_unique<StreamT>(); return *std::get<std::unique_ptr<StreamT>>(payload); }
  std::unordered_map<std::string, double>& zset() { return zset_payload().by_member; }
  std::map<std::pair<double, std::string>, bool>& zord() { return zset_payload().ordered; }
  auto& stream_entries()  { return stream().entries; }
  std::uint64_t& stream_last_ms()  { return stream().last_ms; }
  std::uint64_t& stream_last_seq() { return stream().last_seq; }
  auto& stream_groups()   { return stream().groups; }

  // ── Const accessors (assume the variant already holds the matching type; callers
  //    switch on type() first). std::get throws on a type bug rather than UB. ──
  const std::string& str() const { return std::get<std::string>(payload); }
  const HashT& hash() const { return *std::get<std::unique_ptr<HashT>>(payload); }
  const ListT& list() const { return *std::get<std::unique_ptr<ListT>>(payload); }
  const SetT&  set()  const { return *std::get<std::unique_ptr<SetT>>(payload); }
  const ZSetT& zset_payload() const { return *std::get<std::unique_ptr<ZSetT>>(payload); }
  const StreamT& stream() const { return *std::get<std::unique_ptr<StreamT>>(payload); }
  const std::unordered_map<std::string, double>& zset() const { return zset_payload().by_member; }
  const std::map<std::pair<double, std::string>, bool>& zord() const { return zset_payload().ordered; }
  const auto& stream_entries()  const { return stream().entries; }
  std::uint64_t stream_last_ms()  const { return stream().last_ms; }
  std::uint64_t stream_last_seq() const { return stream().last_seq; }
  const auto& stream_groups()   const { return stream().groups; }

  // Default move; deep-copy so rename/move_key/copy_key/swapdb keep value semantics.
  Entry() = default;
  Entry(Entry&&) = default;
  Entry& operator=(Entry&&) = default;
  Entry(const Entry& o)
      : expire_at_ms(o.expire_at_ms), lru(o.lru), string_force_raw(o.string_force_raw),
        encoding_big(o.encoding_big), encoding_no_intset(o.encoding_no_intset),
        payload(clone_payload(o.payload)) {}
  Entry& operator=(const Entry& o) {
    if (this != &o) {
      expire_at_ms = o.expire_at_ms; lru = o.lru; string_force_raw = o.string_force_raw;
      encoding_big = o.encoding_big; encoding_no_intset = o.encoding_no_intset;
      payload = clone_payload(o.payload);
    }
    return *this;
  }

 private:
  static Payload clone_payload(const Payload& p) {
    return std::visit([](const auto& v) -> Payload {
      using T = std::decay_t<decltype(v)>;
      if constexpr (std::is_same_v<T, std::string>) return v;
      else return std::make_unique<typename T::element_type>(*v);
    }, p);
  }
};

class DataStore {
 public:
  DataStore();

  bool select_db(int db_index);
  int current_db() const;

  std::optional<std::string> get(const std::string& key);
  bool set(const std::string& key, const std::string& value, bool nx, bool xx,
           std::optional<std::int64_t> expire_at_ms, bool keep_ttl);
  std::optional<std::string> getdel(const std::string& key);
  std::optional<std::string> getex(const std::string& key, std::optional<std::int64_t> expire_at_ms,
                                   bool persist);

  bool del(const std::string& key);
  std::size_t exists(const std::vector<std::string>& keys);
  std::string type_of(const std::string& key);
  bool is_wrongtype_for_string(const std::string& key);
  bool is_wrongtype_for_string_noexpire(const std::string& key);
  bool is_wrongtype_for_hash(const std::string& key);
  bool is_wrongtype_for_list(const std::string& key);
  bool is_wrongtype_for_set(const std::string& key);
  bool is_wrongtype_for_zset(const std::string& key);
  bool is_wrongtype_for_stream(const std::string& key);

  std::int64_t pttl(const std::string& key);
  std::int64_t ttl(const std::string& key);
  std::int64_t expiretime(const std::string& key);
  std::int64_t pexpiretime(const std::string& key);

  bool expire(const std::string& key, std::int64_t expire_at_ms);
  bool persist(const std::string& key);

  std::vector<std::string> keys(const std::string& pattern);
  std::optional<std::string> randomkey();

  bool rename(const std::string& src, const std::string& dst, bool nx_only);
  bool swapdb(int a, int b);
  std::int64_t move_key(const std::string& key, int dst_db);

  std::int64_t incrby(const std::string& key, std::int64_t by, bool& ok, std::string& err);
  std::string incrbyfloat(const std::string& key, long double by, bool& ok, std::string& err);
  std::size_t append(const std::string& key, const std::string& value);
  std::size_t strlen(const std::string& key);
  int setbit(const std::string& key, std::uint64_t bit_offset, int bit, bool& ok, std::string& err);
  int getbit(const std::string& key, std::uint64_t bit_offset, bool& ok, std::string& err);
  std::size_t setrange(const std::string& key, std::size_t offset, const std::string& value, bool& ok, std::string& err);
  std::string getrange(const std::string& key, std::int64_t start, std::int64_t stop, bool& ok, std::string& err);
  std::int64_t hset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields, bool& wrongtype);
  std::optional<std::string> hget(const std::string& key, const std::string& field, bool& wrongtype);
  std::int64_t hdel(const std::string& key, const std::vector<std::string>& fields, bool& wrongtype);
  std::int64_t hlen(const std::string& key, bool& wrongtype);
  bool hexists(const std::string& key, const std::string& field, bool& wrongtype);
  std::vector<std::pair<std::string, std::string>> hgetall(const std::string& key, bool& wrongtype);
  std::pair<std::uint64_t, std::vector<std::pair<std::string, std::string>>> hscan(
      const std::string& key, std::uint64_t cursor, std::size_t count, bool& wrongtype);
  std::int64_t lpush(const std::string& key, const std::vector<std::string>& values, bool& wrongtype);
  std::int64_t rpush(const std::string& key, const std::vector<std::string>& values, bool& wrongtype);
  std::optional<std::string> lpop(const std::string& key, bool& wrongtype);
  std::optional<std::string> rpop(const std::string& key, bool& wrongtype);
  std::int64_t llen(const std::string& key, bool& wrongtype);
  std::vector<std::string> lrange(const std::string& key, std::int64_t start, std::int64_t stop, bool& wrongtype);
  std::int64_t sadd(const std::string& key, const std::vector<std::string>& members, bool& wrongtype);
  std::int64_t srem(const std::string& key, const std::vector<std::string>& members, bool& wrongtype);
  bool sismember(const std::string& key, const std::string& member, bool& wrongtype);
  std::vector<std::string> smembers(const std::string& key, bool& wrongtype);
  std::int64_t scard(const std::string& key, bool& wrongtype);
  std::optional<std::string> spop(const std::string& key, bool& wrongtype);
  std::pair<std::uint64_t, std::vector<std::string>> sscan(
      const std::string& key, std::uint64_t cursor, std::size_t count, bool& wrongtype);
  std::int64_t copy_key(const std::string& src, const std::string& dst, int db, bool replace);
  struct ZAddResult {
    bool wrongtype = false;
    bool valid = true;
    bool changed = false;
    bool added = false;
    double score = 0.0;
  };
  ZAddResult zadd_one(const std::string& key, double score, const std::string& member,
                      bool nx, bool xx, bool gt, bool lt, bool incr);
  std::vector<std::pair<std::string, double>> zrange(const std::string& key, std::int64_t start, std::int64_t stop,
                                                     bool withscores, bool& wrongtype);
  std::pair<std::uint64_t, std::vector<std::pair<std::string, double>>> zscan(
      const std::string& key, std::uint64_t cursor, std::size_t count, bool& wrongtype);
  std::vector<std::pair<std::string, double>> zpop(const std::string& key, std::size_t count, bool max, bool& wrongtype);
  std::optional<std::string> xadd(const std::string& key, const std::string& id,
                                  const std::vector<std::pair<std::string, std::string>>& fields,
                                  bool& wrongtype, std::string& err);
  std::int64_t xlen(const std::string& key, bool& wrongtype);
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> xrange(
      const std::string& key, const std::string& start, const std::string& stop, bool rev, bool& wrongtype,
      bool* invalid_id = nullptr);
  // XINFO STREAM summary. first/last are the {id, fields} of the boundary entries (id
  // empty when the stream is empty). Returns false (wrongtype=true) for a non-stream.
  struct StreamInfo {
    std::int64_t length = 0;
    std::string last_id = "0-0";
    std::string max_deleted_id = "0-0";
    std::uint64_t entries_added = 0;
    std::size_t groups = 0;
    std::pair<std::string, std::vector<std::pair<std::string, std::string>>> first;
    std::pair<std::string, std::vector<std::pair<std::string, std::string>>> last;
  };
  bool stream_info(const std::string& key, StreamInfo& out, bool& wrongtype);
  bool xgroup_create(const std::string& key, const std::string& group, const std::string& start_id,
                     bool mkstream, bool& wrongtype, std::string& err);
  bool xgroup_setid(const std::string& key, const std::string& group, const std::string& id,
                    bool& wrongtype, std::string& err);
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>>> xreadgroup(
      const std::string& group, const std::string& consumer, const std::vector<std::pair<std::string, std::string>>& streams,
      bool& wrongtype, std::string& err);
  // is_trim=true removes entries as part of MAXLEN/MINID trimming, which (unlike an
  // explicit XDEL) must NOT advance max-deleted-entry-id — Redis only bumps that on XDEL.
  std::int64_t xdel(const std::string& key, const std::vector<std::string>& ids, bool& wrongtype,
                    bool is_trim = false);
  std::int64_t xack(const std::string& key, const std::string& group, const std::vector<std::string>& ids,
                    bool& wrongtype, std::string& err);
  std::vector<std::string> xpending_summary(const std::string& key, const std::string& group,
                                            bool& wrongtype, std::string& err);

  // ── Stream serialization (for RDB persistence + replication of streams) ──
  struct StreamPel { std::string id; std::string consumer; std::int64_t delivery_ms; std::int64_t delivery_count; };
  struct StreamGroupDump { std::string name; std::string last_delivered_id; std::vector<StreamPel> pending; };
  struct StreamDump {
    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> entries;
    std::uint64_t last_ms = 0;
    std::uint64_t last_seq = 0;
    std::vector<StreamGroupDump> groups;
  };
  bool dump_stream(const std::string& key, StreamDump& out);      // false if absent / not a stream
  void restore_stream(const std::string& key, const StreamDump& d);

  // ── Stream ops (Batch 6) ──
  bool xsetid(const std::string& key, const std::string& id, bool& wrongtype, std::string& err,
              const std::string* entries_added = nullptr, const std::string* max_deleted_id = nullptr);
  // XCLAIM: returns claimed entries (id, fields) for ids that were pending; updates PEL ownership.
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> xclaim(
      const std::string& key, const std::string& group, const std::string& consumer,
      std::int64_t min_idle_ms, const std::vector<std::string>& ids, bool justid, bool force,
      bool& wrongtype, std::string& err);
  // XAUTOCLAIM: from start cursor, claim up to count idle entries; returns {next_cursor, entries, deleted}.
  struct AutoClaimResult {
    std::string next_cursor;
    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> claimed;
    std::vector<std::string> deleted;
  };
  AutoClaimResult xautoclaim(const std::string& key, const std::string& group, const std::string& consumer,
                             std::int64_t min_idle_ms, const std::string& start, std::int64_t count,
                             bool justid, bool& wrongtype, std::string& err);

  void flushall();
  void flushdb();
  // Lazyfree variants: detach the data on the caller (event-loop) thread and destruct it on
  // a background thread, so deleting/flushing a huge dataset doesn't stall the loop. Safe
  // under the single-writer model — ownership is moved out of the live map before handoff.
  bool unlink(const std::string& key);   // like del, but big values are freed in background
  void flushall_async();
  void flushdb_async();
  std::size_t dbsize();
  std::vector<std::string> collect_expired_keys();
  bool lazy_expire_key(const std::string& key);
  // Returns the (db_index, key) pairs physically removed this cycle so the caller
  // (master) can propagate DEL to replicas/AOF and keep replicas in sync.
  std::vector<std::pair<int, std::string>> active_expire_cycle(std::size_t budget_per_db = 64);
  std::pair<std::uint64_t, std::vector<std::string>> scan(std::uint64_t cursor, std::size_t count, const std::string& match = "*", const std::string& type = "");
  std::uint64_t mutation_epoch() const;
  void increment_mutation_epoch();
  // Returns {total_keys, keys_with_expiry, avg_ttl_ms} for current db
  std::tuple<std::size_t, std::size_t, std::int64_t> keyspace_stats() const;

  // Observability counters
  std::int64_t stat_expired_keys() const { return stat_expired_keys_.load(); }
  std::int64_t stat_keyspace_hits() const { return stat_keyspace_hits_.load(); }
  std::int64_t stat_keyspace_misses() const { return stat_keyspace_misses_.load(); }
  std::int64_t stat_evicted_keys() const { return stat_evicted_keys_.load(); }
  void record_hit() { stat_keyspace_hits_.fetch_add(1, std::memory_order_relaxed); }
  void record_miss() { stat_keyspace_misses_.fetch_add(1, std::memory_order_relaxed); }
  void record_expired() { stat_expired_keys_.fetch_add(1, std::memory_order_relaxed); }
  void record_evicted() { stat_evicted_keys_.fetch_add(1, std::memory_order_relaxed); }

  // ── Eviction (maxmemory-policy) ──
  // Update a key's access metadata (LRU clock / LFU counter) used by eviction. Called
  // on the hot read/write paths. lfu=false → LRU (store coarse access time); lfu=true →
  // LFU (probabilistic log-increment counter). Touch is O(1).
  void touch_lru(Entry& e, bool lfu);
  void set_eviction_lfu(bool v) { eviction_lfu_ = v; }  // pick LRU vs LFU touch semantics
  void touch_key_for_eviction(const std::string& key);  // current db; no-op if absent
  struct EvictVictim { int db; std::string key; std::int64_t freed_bytes; };
  // Sample `samples` candidate keys across all DBs (allkeys-*) or only volatile keys
  // (volatile-*), pick the victim per policy, erase it, record the eviction, and decrement
  // the cached used_memory so a tight eviction loop sees memory fall in real time (instead
  // of over-evicting against the ~100ms-stale cache). nullopt = nothing evictable.
  std::optional<EvictVictim> evict_one(MaxmemoryPolicy policy, int samples);

  // Memory accounting — walks all databases to compute an estimate
  std::int64_t estimate_used_memory() const;
  // Bounded-staleness cached view of estimate_used_memory(): recomputed at most
  // once per ~100ms so INFO and per-write maxmemory checks are O(1) amortised
  // instead of O(N+members) on every call. Drift-free (always the ground truth,
  // just possibly up to ~100ms stale). True per-op O(1) accounting is deferred to
  // the Entry tagged-union refactor (see compat/delta.md).
  std::int64_t used_memory() const;
  bool save_snapshot_file(const std::filesystem::path& path, std::string& err);
  bool load_snapshot_file(const std::filesystem::path& path, std::string& err);
  std::vector<std::vector<std::string>> export_aof_commands() const;
  std::optional<std::string> object_encoding(const std::string& key);
  // Redis encoding-transition thresholds (the *-max-listpack-* / set-max-intset-entries
  // config params). Storage stays hashtable-grade for now; OBJECT ENCODING honors these
  // limits + the per-entry sticky conversion flag so assert_encoding tests behave like
  // Redis. Real listpack/intset backends (Phase 6) will make the reporting honest.
  struct EncodingLimits {
    std::int64_t hash_entries = 128;
    std::int64_t hash_value = 64;
    std::int64_t list_size = 128;   // negative Redis size-classes are treated as 128
    std::int64_t set_intset = 512;
    std::int64_t set_entries = 128;
    std::int64_t set_value = 64;
    std::int64_t zset_entries = 128;
    std::int64_t zset_value = 64;
  };
  EncodingLimits& encoding_limits() { return encoding_limits_; }
  std::optional<std::string> debug_digest_value(const std::string& key);
  std::string debug_digest();  // order-independent whole-dataset digest (DEBUG DIGEST)
  // Raw keyspace presence, ignoring logical expiry — DEBUG OBJECT peeks the dict like
  // Redis's debugCommand (an expired-but-unreaped key still reports its object info).
  bool key_exists_raw(const std::string& key) const;
  // OBJECT IDLETIME/FREQ readers over Entry::lru (nullopt = missing/expired key).
  std::optional<std::int64_t> object_idletime_seconds(const std::string& key);
  std::optional<std::int64_t> object_freq(const std::string& key);
  // Legacy accessors kept for existing callers; they delegate to encoding_limits().
  void set_zset_max_ziplist_entries(std::int64_t v);
  std::int64_t zset_max_ziplist_entries() const;
  static void freeze_time();
  static void unfreeze_time();
  static std::int64_t now_ms();

  ~DataStore();

 private:
  using DB = std::unordered_map<std::string, Entry>;

  bool expire_if_needed(DB& db, const std::string& key);
  // Background lazyfree: a worker thread destructs detached maps off the event loop.
  void ensure_lazyfree_thread();
  void lazyfree_enqueue(DB&& db);
  static bool entry_is_big(const Entry& e);  // worth offloading (large collection)?
  // Approximate bytes attributable to one key (map node key + Entry + active payload).
  // Shared by estimate_used_memory() and evict_one() so accounting and eviction agree.
  std::int64_t entry_cost(const std::string& key, const Entry& e) const;

  int current_db_;
  std::vector<DB> dbs_;
  std::uint64_t mutation_epoch_ = 0;
  mutable std::int64_t used_mem_cache_ = -1;   // -1 = not yet computed
  mutable std::int64_t used_mem_cache_ms_ = 0; // wall-clock of last recompute
  EncodingLimits encoding_limits_;
  bool eviction_lfu_ = false;   // touch_key_for_eviction uses LFU semantics when true
  // Lazyfree worker state.
  std::thread lazyfree_thread_;
  std::mutex lazyfree_mu_;
  std::condition_variable lazyfree_cv_;
  std::deque<DB> lazyfree_queue_;
  bool lazyfree_stop_ = false;
  bool lazyfree_started_ = false;
  std::atomic<std::int64_t> stat_expired_keys_{0};
  std::atomic<std::int64_t> stat_keyspace_hits_{0};
  std::atomic<std::int64_t> stat_keyspace_misses_{0};
  std::atomic<std::int64_t> stat_evicted_keys_{0};
  static inline std::int64_t frozen_time_ms_ = 0;
  static inline bool time_frozen_ = false;
};

DataStore& store();

} // namespace peadb
