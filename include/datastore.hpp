#pragma once

#include <cstdint>
#include <optional>
#include <deque>
#include <unordered_set>
#include <map>
#include <tuple>
#include <string>
#include <filesystem>
#include <unordered_map>
#include <vector>

namespace peadb {

enum class ValueType {
  String,
  Hash,
  List,
  Set,
  ZSet,
  Stream,
};

struct Entry {
  struct StreamGroup {
    std::string last_delivered_id = "0-0";
    std::unordered_map<std::string, std::string> pending_to_consumer;
    std::unordered_map<std::string, std::size_t> pending_per_consumer;
  };

  ValueType type = ValueType::String;
  std::string value;
  bool string_force_raw = false;
  std::unordered_map<std::string, std::string> hash_value;
  std::deque<std::string> list_value;
  std::unordered_set<std::string> set_value;
  std::unordered_map<std::string, double> zset_value;
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> stream_entries;
  std::uint64_t stream_last_ms = 0;
  std::uint64_t stream_last_seq = 0;
  std::unordered_map<std::string, StreamGroup> stream_groups;
  std::optional<std::int64_t> expire_at_ms;
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
      const std::string& key, const std::string& start, const std::string& stop, bool rev, bool& wrongtype);
  bool xgroup_create(const std::string& key, const std::string& group, const std::string& start_id,
                     bool mkstream, bool& wrongtype, std::string& err);
  bool xgroup_setid(const std::string& key, const std::string& group, const std::string& id,
                    bool& wrongtype, std::string& err);
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>>> xreadgroup(
      const std::string& group, const std::string& consumer, const std::vector<std::pair<std::string, std::string>>& streams,
      bool& wrongtype, std::string& err);
  std::int64_t xdel(const std::string& key, const std::vector<std::string>& ids, bool& wrongtype);
  std::int64_t xack(const std::string& key, const std::string& group, const std::vector<std::string>& ids,
                    bool& wrongtype, std::string& err);
  std::vector<std::string> xpending_summary(const std::string& key, const std::string& group,
                                            bool& wrongtype, std::string& err);

  void flushall();
  void flushdb();
  std::size_t dbsize();
  std::vector<std::string> collect_expired_keys();
  bool lazy_expire_key(const std::string& key);
  void active_expire_cycle(std::size_t budget_per_db = 64);
  std::pair<std::uint64_t, std::vector<std::string>> scan(std::uint64_t cursor, std::size_t count, const std::string& match = "*", const std::string& type = "");
  std::uint64_t mutation_epoch() const;
  bool save_snapshot_file(const std::filesystem::path& path, std::string& err);
  bool load_snapshot_file(const std::filesystem::path& path, std::string& err);
  std::vector<std::vector<std::string>> export_aof_commands() const;
  std::optional<std::string> object_encoding(const std::string& key);
  std::optional<std::string> debug_digest_value(const std::string& key);
  void set_zset_max_ziplist_entries(std::int64_t v);
  std::int64_t zset_max_ziplist_entries() const;
  static void freeze_time();
  static void unfreeze_time();
  static std::int64_t now_ms();

 private:
  using DB = std::unordered_map<std::string, Entry>;

  bool expire_if_needed(DB& db, const std::string& key);

  int current_db_;
  std::vector<DB> dbs_;
  std::uint64_t mutation_epoch_ = 0;
  std::int64_t zset_max_ziplist_entries_ = 128;
  static inline std::int64_t frozen_time_ms_ = 0;
  static inline bool time_frozen_ = false;
};

DataStore& store();

} // namespace peadb
