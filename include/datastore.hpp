#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <deque>
#include <unordered_set>
#include <map>
#include <tuple>
#include <string>
#include <string_view>
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

#include "dict.hpp"

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
  struct StreamConsumer {
    std::int64_t seen_time = 0;  // last time the consumer attempted any read (XINFO idle)
    std::int64_t active_time =
        -1;  // last time it actually got data/claimed (XINFO inactive; -1=never)
  };
  struct StreamGroup {
    std::string last_delivered_id = "0-0";
    std::int64_t entries_read =
        0;  // logical read counter (XINFO GROUPS entries-read; -1 = unknown)
    std::map<std::string, StreamConsumer> consumers;  // name -> consumer (ordered for XINFO)
    std::unordered_map<std::string, std::string> pending_to_consumer;   // id -> consumer
    std::unordered_map<std::string, std::size_t> pending_per_consumer;  // consumer -> count
    std::unordered_map<std::string, std::int64_t>
        pending_delivery_ms;  // id -> last delivery time (ms)
    std::unordered_map<std::string, std::int64_t> pending_delivery_count;  // id -> delivery count
  };

  // The tables whose elements HSCAN, SSCAN and ZSCAN walk are Dicts (power-of-two buckets), so a
  // scan keeps Redis's guarantee when the collection grows or shrinks (compat/delta.md item 32).
  using HashT = DictMap<std::string, std::string, DictStringHash>;
  using ListT = std::deque<std::string>;
  using SetT = DictSet<std::string, DictStringHash>;
  struct ZSetT {
    DictMap<std::string, double, DictStringHash> by_member;  // member → score (O(1) lookup)
    std::map<std::pair<double, std::string>, bool> ordered;  // {score,member} sorted index
  };
  struct StreamT {
    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> entries;
    std::uint64_t last_ms = 0;
    std::uint64_t last_seq = 0;
    std::uint64_t entries_added = 0;     // total ever XADDed (XINFO entries-added)
    std::string max_deleted_id = "0-0";  // highest id ever XDELed (XINFO max-deleted-entry-id)
    std::unordered_map<std::string, StreamGroup> groups;
  };
  using Payload =
      std::variant<std::string, std::unique_ptr<HashT>, std::unique_ptr<ListT>,
                   std::unique_ptr<SetT>, std::unique_ptr<ZSetT>, std::unique_ptr<StreamT>>;

  std::optional<std::int64_t> expire_at_ms;
  std::uint32_t lru = 0;  // eviction metadata (LRU clock / LFU counter), Phase B
  bool string_force_raw = false;
  // Created and not touched yet: the first touch_lru() starts the LFU counter at Redis's
  // LFU_INIT_VAL (5) instead of counting the write that made the key, as Redis's createObject()
  // does, so a new key is not the first one LFU eviction picks.
  bool lru_fresh = true;
  // Sticky encoding conversion: once a collection is observed past its listpack/intset
  // thresholds it reports the big encoding forever, like Redis's one-way conversions
  // (set on OBJECT ENCODING computation — an approximation until real backends exist).
  bool encoding_big = false;
  // Sets only: permanently off the intset encoding (a non-integer member was added, or
  // the set grew past set-max-intset-entries). Maintained at write time in sadd().
  bool encoding_no_intset = false;
  Payload payload{std::string{}};

  ValueType type() const {
    return static_cast<ValueType>(payload.index());
  }

  // ── Mutable accessors (materialize the active payload, destroying any prior one) ──
  std::string& str() {
    if (payload.index() != 0) payload.emplace<std::string>();
    return std::get<std::string>(payload);
  }
  HashT& hash() {
    if (payload.index() != 1) payload = std::make_unique<HashT>();
    return *std::get<std::unique_ptr<HashT>>(payload);
  }
  ListT& list() {
    if (payload.index() != 2) payload = std::make_unique<ListT>();
    return *std::get<std::unique_ptr<ListT>>(payload);
  }
  SetT& set() {
    if (payload.index() != 3) payload = std::make_unique<SetT>();
    return *std::get<std::unique_ptr<SetT>>(payload);
  }
  ZSetT& zset_payload() {
    if (payload.index() != 4) payload = std::make_unique<ZSetT>();
    return *std::get<std::unique_ptr<ZSetT>>(payload);
  }
  StreamT& stream() {
    if (payload.index() != 5) payload = std::make_unique<StreamT>();
    return *std::get<std::unique_ptr<StreamT>>(payload);
  }
  DictMap<std::string, double, DictStringHash>& zset() {
    return zset_payload().by_member;
  }
  std::map<std::pair<double, std::string>, bool>& zord() {
    return zset_payload().ordered;
  }
  auto& stream_entries() {
    return stream().entries;
  }
  std::uint64_t& stream_last_ms() {
    return stream().last_ms;
  }
  std::uint64_t& stream_last_seq() {
    return stream().last_seq;
  }
  auto& stream_groups() {
    return stream().groups;
  }

  // ── Const accessors (assume the variant already holds the matching type; callers
  //    switch on type() first). std::get throws on a type bug rather than UB. ──
  const std::string& str() const {
    return std::get<std::string>(payload);
  }
  const HashT& hash() const {
    return *std::get<std::unique_ptr<HashT>>(payload);
  }
  const ListT& list() const {
    return *std::get<std::unique_ptr<ListT>>(payload);
  }
  const SetT& set() const {
    return *std::get<std::unique_ptr<SetT>>(payload);
  }
  const ZSetT& zset_payload() const {
    return *std::get<std::unique_ptr<ZSetT>>(payload);
  }
  const StreamT& stream() const {
    return *std::get<std::unique_ptr<StreamT>>(payload);
  }
  const DictMap<std::string, double, DictStringHash>& zset() const {
    return zset_payload().by_member;
  }
  const std::map<std::pair<double, std::string>, bool>& zord() const {
    return zset_payload().ordered;
  }
  const auto& stream_entries() const {
    return stream().entries;
  }
  std::uint64_t stream_last_ms() const {
    return stream().last_ms;
  }
  std::uint64_t stream_last_seq() const {
    return stream().last_seq;
  }
  const auto& stream_groups() const {
    return stream().groups;
  }

  // Default move; deep-copy so rename/move_key/copy_key/swapdb keep value semantics.
  Entry() = default;
  Entry(Entry&&) = default;
  Entry& operator=(Entry&&) = default;
  Entry(const Entry& o)
      : expire_at_ms(o.expire_at_ms),
        lru(o.lru),
        string_force_raw(o.string_force_raw),
        lru_fresh(o.lru_fresh),
        encoding_big(o.encoding_big),
        encoding_no_intset(o.encoding_no_intset),
        payload(clone_payload(o.payload)) {
  }
  Entry& operator=(const Entry& o) {
    if (this != &o) {
      expire_at_ms = o.expire_at_ms;
      lru = o.lru;
      string_force_raw = o.string_force_raw;
      lru_fresh = o.lru_fresh;
      encoding_big = o.encoding_big;
      encoding_no_intset = o.encoding_no_intset;
      payload = clone_payload(o.payload);
    }
    return *this;
  }

 private:
  static Payload clone_payload(const Payload& p) {
    return std::visit(
        [](const auto& v) -> Payload {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::string>)
            return v;
          else
            return std::make_unique<typename T::element_type>(*v);
        },
        p);
  }
};

class DataStore {
 public:
  DataStore();

  bool select_db(int db_index);
  int current_db() const;

  std::optional<std::string> get(const std::string& key);
  // GET: one lookup that also reports a key of another type (counted as a hit, as Redis's
  // lookupKeyRead() counts it before checkType()).
  std::optional<std::string> get_string(const std::string& key, bool& wrongtype);
  bool set(const std::string& key, const std::string& value, bool nx, bool xx,
           std::optional<std::int64_t> expire_at_ms, bool keep_ttl);
  std::optional<std::string> getdel(const std::string& key);

  bool del(const std::string& key);
  // Erase `key` whether or not its TTL has passed, with no lazy-expiry DEL or event (Redis's
  // dbDelete(), as RESTORE ... REPLACE uses it).
  bool del_raw(const std::string& key);
  std::size_t exists(const std::vector<std::string>& keys);
  std::string type_of(const std::string& key);
  bool is_wrongtype_for_string(const std::string& key);
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

  // Redis's keysCommand(): the keys that match, without the expired ones, which stay in place
  // (KEYS deletes nothing and propagates nothing).
  std::vector<std::string> keys(const std::string& pattern);
  // Walks every key of the current database, expired ones too, calling `fn` with each `const
  // std::string&` in turn — no vector, no per-key copy, and nothing is deleted, like a walk of
  // Redis's slot index (CLUSTER COUNTKEYSINSLOT/GETKEYSINSLOT count and list expired keys).
  //
  // Contract: `fn` must not mutate this DataStore (directly, or through any callback back into
  // it) and must not retain the passed reference past the call — it aliases the live map entry's
  // key, not a copy.
  void for_each_key(const std::function<void(const std::string&)>& fn);
  // Redis's dbRandomKey(): a random live key. On a master the expired keys met on the way are
  // deleted (see Lazy expiry below); on a replica, where nothing is deleted, an expired key is
  // returned when every key has expired, as Redis does.
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
  std::size_t setrange(const std::string& key, std::size_t offset, const std::string& value,
                       bool& ok, std::string& err);
  std::string getrange(const std::string& key, std::int64_t start, std::int64_t stop, bool& ok,
                       std::string& err);
  std::int64_t hset(const std::string& key,
                    const std::vector<std::pair<std::string, std::string>>& fields,
                    bool& wrongtype);
  std::optional<std::string> hget(const std::string& key, const std::string& field,
                                  bool& wrongtype);
  std::int64_t hdel(const std::string& key, const std::vector<std::string>& fields,
                    bool& wrongtype);
  std::int64_t hlen(const std::string& key, bool& wrongtype);
  bool hexists(const std::string& key, const std::string& field, bool& wrongtype);
  std::vector<std::pair<std::string, std::string>> hgetall(const std::string& key, bool& wrongtype);
  // HSCAN, SSCAN and ZSCAN (see scan()): a missing key gives cursor 0 and nothing. A hash, set or
  // sorted set that PeaDB reports with a small encoding (listpack or intset, see object_encoding())
  // is returned whole with cursor 0, whatever the cursor and `count`, as Redis returns those (an
  // intset in ascending order and a sorted set in score order, as Redis keeps them; a small hash or
  // set in table order, where Redis keeps insertion order, compat/delta.md item 13).
  std::pair<std::uint64_t, std::vector<std::pair<std::string, std::string>>> hscan(
      const std::string& key, std::uint64_t cursor, std::int64_t count,
      const std::optional<std::string>& pattern, bool& wrongtype);
  std::int64_t lpush(const std::string& key, const std::vector<std::string>& values,
                     bool& wrongtype);
  std::int64_t rpush(const std::string& key, const std::vector<std::string>& values,
                     bool& wrongtype);
  std::optional<std::string> lpop(const std::string& key, bool& wrongtype);
  std::optional<std::string> rpop(const std::string& key, bool& wrongtype);
  std::int64_t llen(const std::string& key, bool& wrongtype);
  std::vector<std::string> lrange(const std::string& key, std::int64_t start, std::int64_t stop,
                                  bool& wrongtype);
  // In-place list edits, like Redis's t_list.c: the key keeps its Entry (TTL, LRU/LFU) and is
  // deleted only when the list becomes empty, which `key_removed` reports.
  enum class LsetResult { Ok, NoKey, WrongType, OutOfRange };
  LsetResult lset(const std::string& key, std::int64_t index, const std::string& value);
  // count > 0 removes from the head, count < 0 from the tail, 0 removes every match.
  std::int64_t lrem(const std::string& key, std::int64_t count, const std::string& value,
                    bool& wrongtype, bool& key_removed);
  // Returns the new length, -1 when the pivot is not found, 0 when the key does not exist.
  std::int64_t linsert(const std::string& key, bool before, const std::string& pivot,
                       const std::string& value, bool& wrongtype);
  // Returns false when the key does not exist.
  bool ltrim(const std::string& key, std::int64_t start, std::int64_t stop, bool& wrongtype,
             bool& key_removed);
  std::int64_t sadd(const std::string& key, const std::vector<std::string>& members,
                    bool& wrongtype);
  std::int64_t srem(const std::string& key, const std::vector<std::string>& members,
                    bool& wrongtype);
  bool sismember(const std::string& key, const std::string& member, bool& wrongtype);
  std::vector<std::string> smembers(const std::string& key, bool& wrongtype);
  std::int64_t scard(const std::string& key, bool& wrongtype);
  std::optional<std::string> spop(const std::string& key, bool& wrongtype);
  std::pair<std::uint64_t, std::vector<std::string>> sscan(
      const std::string& key, std::uint64_t cursor, std::int64_t count,
      const std::optional<std::string>& pattern, bool& wrongtype);
  std::int64_t copy_key(const std::string& src, const std::string& dst, int db, bool replace);
  struct ZAddResult {
    bool wrongtype = false;
    bool valid = true;
    bool changed = false;
    bool added = false;
    double score = 0.0;
  };
  ZAddResult zadd_one(const std::string& key, double score, const std::string& member, bool nx,
                      bool xx, bool gt, bool lt, bool incr);
  // Removes members in place (the key keeps its TTL); deletes the key when the zset becomes
  // empty, which `key_removed` reports.
  std::int64_t zrem(const std::string& key, const std::vector<std::string>& members,
                    bool& wrongtype, bool& key_removed);
  std::vector<std::pair<std::string, double>> zrange(const std::string& key, std::int64_t start,
                                                     std::int64_t stop, bool withscores,
                                                     bool& wrongtype);
  std::pair<std::uint64_t, std::vector<std::pair<std::string, double>>> zscan(
      const std::string& key, std::uint64_t cursor, std::int64_t count,
      const std::optional<std::string>& pattern, bool& wrongtype);
  std::vector<std::pair<std::string, double>> zpop(const std::string& key, std::size_t count,
                                                   bool max, bool& wrongtype);
  std::optional<std::string> xadd(const std::string& key, const std::string& id,
                                  const std::vector<std::pair<std::string, std::string>>& fields,
                                  bool& wrongtype, std::string& err);
  std::int64_t xlen(const std::string& key, bool& wrongtype);
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> xrange(
      const std::string& key, const std::string& start, const std::string& stop, bool rev,
      bool& wrongtype, bool* invalid_id = nullptr);
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
  // start_id may be "$" (resolved to the stream's current last-id). entries_read, when
  // non-null, seeds the group's read counter (already validated >= -1 by the caller).
  bool xgroup_create(const std::string& key, const std::string& group, const std::string& start_id,
                     bool mkstream, bool& wrongtype, std::string& err,
                     const std::int64_t* entries_read = nullptr);
  bool xgroup_setid(const std::string& key, const std::string& group, const std::string& id,
                    bool& wrongtype, std::string& err, const std::int64_t* entries_read = nullptr);
  // count=0 means "no limit". noack skips adding delivered entries to the PEL. An idreq of
  // ">" delivers new entries (advancing the group's last-delivered-id and, unless noack,
  // adding to the consumer's PEL); any other id is a history read of the consumer's own PEL
  // (id >= start), which re-emits pending entries — a returned entry with EMPTY fields is
  // the null-fields sentinel for a pending id whose stream entry was since removed.
  std::vector<std::pair<
      std::string,
      std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>>>
  xreadgroup(const std::string& group, const std::string& consumer,
             const std::vector<std::pair<std::string, std::string>>& streams, std::size_t count,
             bool noack, bool& wrongtype, std::string& err, bool* consumer_created = nullptr);
  // Fetch a group's current last-delivered-id and entries-read (for NOACK propagation).
  bool stream_group_ids(const std::string& key, const std::string& group, std::string& last_id,
                        std::int64_t& entries_read);
  // is_trim=true removes entries as part of MAXLEN/MINID trimming, which (unlike an
  // explicit XDEL) must NOT advance max-deleted-entry-id — Redis only bumps that on XDEL.
  std::int64_t xdel(const std::string& key, const std::vector<std::string>& ids, bool& wrongtype,
                    bool is_trim = false);
  std::int64_t xack(const std::string& key, const std::string& group,
                    const std::vector<std::string>& ids, bool& wrongtype, std::string& err);
  // Summary form: returns {total, min_id, max_id} and fills `consumers` with the per-consumer
  // {name, count} breakdown (Redis XPENDING key group).
  std::vector<std::string> xpending_summary(
      const std::string& key, const std::string& group,
      std::vector<std::pair<std::string, std::int64_t>>& consumers, bool& wrongtype,
      std::string& err);
  // Extended form: one entry per pending message in [start,end], optionally restricted to a
  // consumer and to messages idle for at least min_idle_ms, ordered by id, capped at count.
  struct PendingItem {
    std::string id;
    std::string consumer;
    std::int64_t idle_ms;
    std::int64_t delivery_count;
  };
  std::vector<PendingItem> xpending_extended(const std::string& key, const std::string& group,
                                             const std::string& start, const std::string& end,
                                             std::int64_t count, const std::string* consumer,
                                             std::int64_t min_idle_ms, bool& wrongtype,
                                             std::string& err);

  // ── Consumer-group management ──
  int xgroup_destroy(const std::string& key, const std::string& group, bool& wrongtype);
  int xgroup_createconsumer(const std::string& key, const std::string& group,
                            const std::string& consumer, bool& wrongtype, std::string& err);
  std::int64_t xgroup_delconsumer(const std::string& key, const std::string& group,
                                  const std::string& consumer, bool& wrongtype, std::string& err,
                                  bool* existed = nullptr);

  // ── XINFO ──
  // entries_read == -1 means "unknown" (reported as nil). lag_valid=false → lag is nil.
  struct GroupInfo {
    std::string name, last_delivered_id;
    std::int64_t entries_read;
    bool lag_valid;
    std::int64_t lag;
    std::size_t pending;
    std::size_t consumers;
  };
  std::vector<GroupInfo> xinfo_groups(const std::string& key, bool& wrongtype, std::string& err);
  struct ConsumerInfo {
    std::string name;
    std::size_t pending;
    std::int64_t idle;
    std::int64_t inactive;
  };
  std::vector<ConsumerInfo> xinfo_consumers(const std::string& key, const std::string& group,
                                            bool& wrongtype, std::string& err);
  struct StreamFullPel {
    std::string id;
    std::int64_t delivery_ms;
    std::int64_t delivery_count;
  };
  struct StreamFullConsumer {
    std::string name;
    std::int64_t seen_time;
    std::int64_t active_time;
    std::vector<StreamFullPel> pending;
  };
  struct StreamFullGroupPel {
    std::string id;
    std::string consumer;
    std::int64_t delivery_ms;
    std::int64_t delivery_count;
  };
  struct StreamFullGroup {
    std::string name, last_delivered_id;
    std::int64_t entries_read;
    bool lag_valid;
    std::int64_t lag;
    std::int64_t pel_count;
    std::vector<StreamFullGroupPel> pending;
    std::vector<StreamFullConsumer> consumers;
  };
  struct StreamFullInfo {
    std::int64_t length;
    std::string last_id, max_deleted_id, recorded_first_id;
    std::int64_t entries_added;
    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> entries;
    std::vector<StreamFullGroup> groups;
  };
  bool xinfo_stream_full(const std::string& key, std::size_t count, StreamFullInfo& out,
                         bool& wrongtype);

  // ── Stream serialization (for RDB persistence + replication of streams) ──
  struct StreamPel {
    std::string id;
    std::string consumer;
    std::int64_t delivery_ms;
    std::int64_t delivery_count;
  };
  struct StreamGroupDump {
    std::string name;
    std::string last_delivered_id;
    std::vector<StreamPel> pending;
  };
  struct StreamDump {
    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> entries;
    std::uint64_t last_ms = 0;
    std::uint64_t last_seq = 0;
    std::vector<StreamGroupDump> groups;
  };
  bool dump_stream(const std::string& key, StreamDump& out);  // false if absent / not a stream
  void restore_stream(const std::string& key, const StreamDump& d);

  // ── Stream ops (Batch 6) ──
  bool xsetid(const std::string& key, const std::string& id, bool& wrongtype, std::string& err,
              const std::string* entries_added = nullptr,
              const std::string* max_deleted_id = nullptr);
  // XCLAIM: returns claimed entries (id, fields) for ids that were pending; updates PEL ownership.
  // last_id, when non-null, advances the group's last-delivered-id if greater (XCLAIM LASTID),
  // so a replayed/replicated XCLAIM restores the group cursor an ACK read had advanced.
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> xclaim(
      const std::string& key, const std::string& group, const std::string& consumer,
      std::int64_t min_idle_ms, const std::vector<std::string>& ids, bool justid, bool force,
      bool& wrongtype, std::string& err, const std::string* last_id = nullptr,
      bool* consumer_created = nullptr);
  // XAUTOCLAIM: from start cursor, claim up to count idle entries; returns {next_cursor, entries,
  // deleted}.
  struct AutoClaimResult {
    std::string next_cursor;
    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> claimed;
    std::vector<std::string> deleted;
  };
  AutoClaimResult xautoclaim(const std::string& key, const std::string& group,
                             const std::string& consumer, std::int64_t min_idle_ms,
                             const std::string& start, std::int64_t count, bool justid,
                             bool& wrongtype, std::string& err, bool* consumer_created = nullptr);

  void flushall();
  void flushdb();
  // Lazyfree variants: detach the data on the caller (event-loop) thread and destruct it on
  // a background thread, so deleting/flushing a huge dataset doesn't stall the loop. Safe
  // under the single-writer model — ownership is moved out of the live map before handoff.
  bool unlink(const std::string& key);  // like del, but big values are freed in background
  void flushall_async();
  void flushdb_async();
  std::size_t dbsize();
  // Deletes expired keys of every database within the budget, each through the lazy-expire hook
  // (so its DEL and `expired` event go out at once, as Redis's activeExpireCycle() sends them),
  // and returns the (db_index, key) pairs it deleted. Only a master runs it.
  std::vector<std::pair<int, std::string>> active_expire_cycle(std::size_t budget_per_db = 64);
  // One SCAN call, like Redis's scanGenericCommand(): Dict::scan() steps from `cursor` until
  // `count` keys have been looked at (before the MATCH filter), count * 10 more buckets have been
  // visited, or the scan is over (next cursor 0). Of the keys that match `pattern` (none: every
  // key), an expired one is deleted (expireIfNeeded()); with `type` (lower case) each one is looked
  // up like a read, which counts a keyspace hit or miss, and kept only when it has that type. Every
  // key present for the whole scan is returned at least once, however the database grows or shrinks
  // between calls.
  std::pair<std::uint64_t, std::vector<std::string>> scan(std::uint64_t cursor, std::int64_t count,
                                                          const std::optional<std::string>& pattern,
                                                          const std::optional<std::string>& type);
  // Redis's tryResizeHashTables() for every database: shrinks a keyspace table that is less than
  // 10% full. The caller runs it between commands, and not while a fork child runs (a rehash would
  // make the kernel copy the child's memory pages).
  void shrink_sparse_tables();
  std::uint64_t mutation_epoch() const;
  void increment_mutation_epoch();
  // Returns {total_keys, keys_with_expiry, avg_ttl_ms} for current db
  std::tuple<std::size_t, std::size_t, std::int64_t> keyspace_stats() const;

  // CLIENT PAUSE stops key expiry (Redis's PAUSE_ACTION_EXPIRE). While it is stopped, a lookup
  // that finds an expired key hides the key instead of deleting it: every command sees it as
  // missing, DBSIZE and INFO still count it (as Redis's dict does), and no DEL or `expired`
  // statistic is produced. When expiry starts again the hidden keys go back into their database
  // (unless a write made a new key of the same name meanwhile), so the next access or the
  // active expiry cycle expires them with their DEL then.
  void set_expire_paused(bool paused);
  bool expire_paused() const {
    return expire_paused_;
  }

  // ── Lazy expiry (Redis's expireIfNeeded()) ──
  // What a command does with a key whose TTL has passed (strictly: now > expire time, like
  // keyIsExpired()). On a master it deletes the key, counts it in expired_keys and calls the
  // lazy-expire hook, which propagates the DEL to the AOF and the replicas and sends the `expired`
  // event (Redis's deleteExpiredKeyAndPropagate()); inside a command the DEL joins the command's
  // execution unit, before the command's own write. On a replica (set_replica_expiry(true)) the
  // key is only hidden from the command and kept, because its master's DEL decides: a replica
  // whose clock runs ahead, or whose stream lags, must not drop a key its master kept alive. The
  // keys a command hid go back when the command ends (restore_hidden_expired()). While lazy expiry
  // is disabled (LazyExpireDisabled: loading the AOF or an RDB, applying the master's stream,
  // writing an RDB) no key counts as expired, as in Redis while loading and for its master client.
  using LazyExpireHook = std::function<void(int db, const std::string& key)>;
  void set_lazy_expire_hook(LazyExpireHook hook) {
    lazy_expire_hook_ = std::move(hook);
  }
  void set_replica_expiry(bool replica);
  bool replica_expiry() const {
    return replica_expiry_;
  }
  bool lazy_expire_disabled() const {
    return lazy_expire_disabled_ > 0;
  }
  void restore_hidden_expired() {
    if (has_replica_hidden_) restore_replica_hidden();
  }
  class LazyExpireDisabled {
   public:
    explicit LazyExpireDisabled(DataStore& ds) : ds_(ds) {
      if (ds_.lazy_expire_disabled_++ != 0) return;
      // Every key is back in its database, so an RDB save writes the keys a replica's command or
      // a pause hid too; a later lookup hides them again.
      ds_.restore_hidden_expired();
      if (ds_.expire_paused_) ds_.restore_paused_hidden();  // the pause's map is empty otherwise
    }
    ~LazyExpireDisabled() {
      --ds_.lazy_expire_disabled_;
    }
    LazyExpireDisabled(const LazyExpireDisabled&) = delete;
    LazyExpireDisabled& operator=(const LazyExpireDisabled&) = delete;

   private:
    DataStore& ds_;
  };
  // Lookups without any effect (Redis's LOOKUP_NOEFFECTS): an expired key reads as missing and is
  // neither deleted nor propagated. live_type() is the check a blocked client's wakeup makes;
  // watch_digest() is what WATCH and EXEC compare (Redis's watch code only asks keyIsExpired()).
  std::optional<ValueType> live_type(const std::string& key) const;
  std::optional<std::string> watch_digest(const std::string& key) const;

  // Observability counters
  std::int64_t stat_expired_keys() const {
    return stat_expired_keys_.load();
  }
  std::int64_t stat_keyspace_hits() const {
    return stat_keyspace_hits_.load();
  }
  std::int64_t stat_keyspace_misses() const {
    return stat_keyspace_misses_.load();
  }
  std::int64_t stat_evicted_keys() const {
    return stat_evicted_keys_.load();
  }
  void record_hit() {
    stat_keyspace_hits_.fetch_add(1, std::memory_order_relaxed);
  }
  void record_miss() {
    stat_keyspace_misses_.fetch_add(1, std::memory_order_relaxed);
  }
  void record_expired() {
    stat_expired_keys_.fetch_add(1, std::memory_order_relaxed);
  }
  void record_evicted() {
    stat_evicted_keys_.fetch_add(1, std::memory_order_relaxed);
  }

  // ── Eviction (maxmemory-policy) ──
  // Update a key's access metadata (LRU clock / LFU counter) used by eviction. Called
  // on the hot read/write paths. lfu=false → LRU (store coarse access time); lfu=true →
  // LFU (probabilistic log-increment counter). Touch is O(1).
  void touch_lru(Entry& e, bool lfu);
  void set_eviction_lfu(bool v) {
    eviction_lfu_ = v;
  }  // pick LRU vs LFU touch semantics
  // lfu-log-factor: how slowly the LFU access counter grows (Redis default 10).
  void set_lfu_log_factor(int v) {
    lfu_log_factor_ = v;
  }
  int lfu_log_factor() const {
    return lfu_log_factor_;
  }
  void touch_key_for_eviction(const std::string& key);  // current db; no-op if absent
  struct EvictVictim {
    int db;
    std::string key;
  };
  // Sample `samples` candidate keys across all DBs (allkeys-*) or only volatile keys
  // (volatile-*), pick the victim per policy, erase it (its memory is freed at once) and record
  // the eviction. nullopt = nothing evictable.
  std::optional<EvictVictim> evict_one(MaxmemoryPolicy policy, int samples);

  // Keys handed to the lazyfree thread (UNLINK of a big value, FLUSHALL/FLUSHDB ASYNC) and not
  // freed yet: INFO's lazyfree_pending_objects, and what eviction waits for when it has nothing
  // left to evict (Redis's bioPendingJobsOfType(BIO_LAZY_FREE)).
  std::size_t lazyfree_pending_objects() const {
    return lazyfree_pending_.load(std::memory_order_relaxed);
  }
  bool save_snapshot_file(const std::filesystem::path& path, std::string& err);
  bool load_snapshot_file(const std::filesystem::path& path, std::string& err);
  std::vector<std::vector<std::string>> export_aof_commands() const;
  std::optional<std::string> object_encoding(const std::string& key);
  // Redis encoding-transition thresholds (the *-max-listpack-* / set-max-intset-entries
  // config params). Storage stays hashtable-grade for now; OBJECT ENCODING honors these
  // limits + the per-entry sticky conversion flag so assert_encoding tests behave like
  // Redis. Real listpack/intset backends (Phase 6) will make the reporting honest.
  struct EncodingLimits {  // defaults are Redis 7.2.5's
    std::int64_t hash_entries = 512;
    std::int64_t hash_value = 64;
    std::int64_t list_size = -2;  // > 0: entries per listpack; -1..-5: 4 KB..64 KB of entries
    std::int64_t set_intset = 512;
    std::int64_t set_entries = 128;
    std::int64_t set_value = 64;
    std::int64_t zset_entries = 128;
    std::int64_t zset_value = 64;
  };
  EncodingLimits& encoding_limits() {
    return encoding_limits_;
  }
  std::optional<std::string> debug_digest_value(const std::string& key);
  std::string debug_digest();  // order-independent whole-dataset digest (DEBUG DIGEST)
  // Raw keyspace presence, ignoring logical expiry — DEBUG OBJECT peeks the dict like
  // Redis's debugCommand (an expired-but-unreaped key still reports its object info).
  bool key_exists_raw(const std::string& key) const;
  // OBJECT IDLETIME/FREQ readers over Entry::lru (nullopt = missing/expired key).
  std::optional<std::int64_t> object_idletime_seconds(const std::string& key);
  std::optional<std::uint32_t> object_lru_clock(const std::string& key);
  std::optional<std::int64_t> object_freq(const std::string& key);
  // RESTORE IDLETIME/FREQ: seed a key's LRU idle time (seconds) or LFU frequency counter.
  bool set_object_idle(const std::string& key, std::int64_t idle_seconds);
  bool set_object_freq(const std::string& key, std::int64_t freq);
  // Legacy accessors kept for existing callers; they delegate to encoding_limits().
  void set_zset_max_ziplist_entries(std::int64_t v);
  std::int64_t zset_max_ziplist_entries() const;
  static void freeze_time();
  static void unfreeze_time();
  static std::int64_t now_ms();

  ~DataStore();

 private:
  // The keyspace of one database: a Dict, so SCAN's cursor keeps Redis's guarantee across resizes.
  using DB = DictMap<std::string, Entry, DictStringHash, std::equal_to<std::string>,
                     KeyspaceExpandAllowed>;

  // Redis's keyIsExpired(): the key has a TTL that has passed, and lazy expiry is not disabled.
  bool logically_expired(const Entry& e, std::int64_t now) const {
    return e.expire_at_ms.has_value() && now > *e.expire_at_ms && lazy_expire_disabled_ == 0;
  }
  // The same for one lookup: the clock is read only for a key that has a TTL (the hot path, since
  // most keys have none).
  bool logically_expired(const Entry& e) const {
    return e.expire_at_ms.has_value() && logically_expired(e, now_ms());
  }
  // Redis's expireIfNeeded(): true when `key` has expired, after removing it (remove_expired()).
  bool expire_if_needed(DB& db, const std::string& key);
  // Removes the expired entry at `it` from `db`: on a replica hides it until the command ends;
  // while expiry is paused hides it until the pause ends (see set_expire_paused()); otherwise
  // deletes it, counts it in expired_keys and calls the lazy-expire hook.
  void remove_expired(DB& db, DB::iterator it);
  // The expired keys RANDOMKEY's walk met go through expire_if_needed() after it.
  void expire_met_keys(DB& db, const std::vector<std::string>& keys);
  void restore_replica_hidden();
  void restore_paused_hidden();
  std::size_t db_index_of(const DB& db) const {
    return static_cast<std::size_t>(&db - dbs_.data());
  }
  // Background lazyfree: a worker thread destructs detached maps off the event loop.
  void ensure_lazyfree_thread();
  void lazyfree_enqueue(DB&& db);
  static bool entry_is_big(const Entry& e);  // worth offloading (large collection)?
  // The encoding OBJECT ENCODING reports for `e`; seeing a big encoding sets e.encoding_big.
  const char* encoding_of(Entry& e);
  // Whether HSCAN/SSCAN/ZSCAN return `e` whole: Redis does so for a listpack or intset encoding.
  bool scans_whole(Entry& e) {
    const std::string_view enc = encoding_of(e);
    return enc == "listpack" || enc == "intset";
  }
  int current_db_;
  std::vector<DB> dbs_;
  bool expire_paused_ = false;
  std::vector<DB> hidden_expired_;  // per database: expired keys hidden while expiry is paused
  LazyExpireHook lazy_expire_hook_;
  bool replica_expiry_ = false;
  int lazy_expire_disabled_ = 0;
  std::vector<DB> replica_hidden_;  // per database: expired keys a replica's command hid
  bool has_replica_hidden_ = false;
  std::uint64_t mutation_epoch_ = 0;
  EncodingLimits encoding_limits_;
  bool eviction_lfu_ = false;  // touch_key_for_eviction uses LFU semantics when true
  int lfu_log_factor_ = 10;
  // Lazyfree worker state.
  std::thread lazyfree_thread_;
  std::mutex lazyfree_mu_;
  std::condition_variable lazyfree_cv_;
  std::deque<DB> lazyfree_queue_;
  std::atomic<std::size_t> lazyfree_pending_{0};  // keys queued or being freed by the thread
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

}  // namespace peadb
