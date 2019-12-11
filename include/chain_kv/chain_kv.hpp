// copyright defined in LICENSE

#pragma once

#include <optional>
#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <stdexcept>

namespace chain_kv {

class exception : public std::exception {
   std::string msg;

 public:
   exception(std::string&& msg) : msg(std::move(msg)) {}
   exception(const exception&) = default;
   exception(exception&&)      = default;

   exception& operator=(const exception&) = default;
   exception& operator=(exception&&) = default;

   const char* what() const noexcept override { return msg.c_str(); }
};

inline void check(rocksdb::Status s, const char* prefix) {
   if (!s.ok())
      throw exception(prefix + s.ToString());
}

inline rocksdb::Slice to_slice(const std::vector<char>& v) { return { v.data(), v.size() }; }

template <typename A, typename B>
inline int compare_blob(const A& a, const B& b) {
   static_assert(std::is_same_v<std::decay_t<decltype(*a.data())>, char> ||
                 std::is_same_v<std::decay_t<decltype(*a.data())>, unsigned char>);
   static_assert(std::is_same_v<std::decay_t<decltype(*b.data())>, char> ||
                 std::is_same_v<std::decay_t<decltype(*b.data())>, unsigned char>);
   auto r = memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
   if (r)
      return r;
   if (a.size() < b.size())
      return -1;
   if (a.size() > b.size())
      return 1;
   return 0;
}

using bytes = std::vector<char>;

struct less_blob {
   using is_transparent = void;

   template <typename A, typename B>
   bool operator()(const A& a, const B& b) const {
      return compare_blob(a, b) < 0;
   }
};

template <typename T>
auto append_key(std::vector<char>& dest, T value) -> std::enable_if_t<std::is_arithmetic_v<T>, void> {
   char buf[sizeof(value)];
   memcpy(buf, &value, sizeof(value));
   std::reverse(std::begin(buf), std::end(buf));
   dest.insert(dest.end(), std::begin(buf), std::end(buf));
}

struct database {
   std::unique_ptr<rocksdb::DB> rdb;

   database(const char* db_path, bool create_if_missing, std::optional<uint32_t> threads,
            std::optional<uint32_t> max_open_files) {
      rocksdb::DB*     p;
      rocksdb::Options options;
      options.create_if_missing = create_if_missing;

      options.level_compaction_dynamic_level_bytes = true;
      options.max_background_compactions           = 4;
      options.max_background_flushes               = 2;
      options.bytes_per_sync                       = 1048576;
      options.compaction_pri                       = rocksdb::kMinOverlappingRatio;

      if (threads)
         options.IncreaseParallelism(*threads);
      options.OptimizeLevelStyleCompaction(256ull << 20);
      for (auto& x : options.compression_per_level) // todo: fix snappy build
         x = rocksdb::kNoCompression;

      if (max_open_files)
         options.max_open_files = *max_open_files;

      rocksdb::BlockBasedTableOptions table_options;
      table_options.format_version               = 4;
      table_options.index_block_restart_interval = 16;
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));

      check(rocksdb::DB::Open(options, db_path, &p), "rocksdb::DB::Open: ");
      rdb.reset(p);
   }

   database(database&&) = default;
   database& operator=(database&&) = default;

   void flush(bool allow_write_stall, bool wait) {
      rocksdb::FlushOptions op;
      op.allow_write_stall = allow_write_stall;
      op.wait              = wait;
      rdb->Flush(op);
   }

   void write(rocksdb::WriteBatch& batch) {
      rocksdb::WriteOptions opt;
      opt.disableWAL = true;
      check(rdb->Write(opt, &batch), "write batch");
      batch.Clear();
   }

}; // database

struct key_value {
   rocksdb::Slice key   = {};
   rocksdb::Slice value = {};
};

struct key_present_value {
   rocksdb::Slice key     = {};
   bool           present = {};
   rocksdb::Slice value   = {};
};

template <typename T>
int compare_key(const std::optional<T>& a, const std::optional<T>& b) {
   if (!a && !b)
      return 0;
   else if (!a && b)
      return 1;
   else if (a && !b)
      return -1;
   else
      return compare_blob(a->key, b->key);
}

inline const std::optional<key_present_value>& key_min(const std::optional<key_present_value>& a,
                                                       const std::optional<key_present_value>& b) {
   auto cmp = compare_key(a, b);
   if (cmp <= 0)
      return a;
   else
      return b;
}

struct cached_value {
   std::optional<bytes> current_value = {};
};

using cache_map = std::map<bytes, cached_value, less_blob>;

class view {
 public:
   class iterator;

   database& db;

 private:
   rocksdb::WriteBatch write_batch;
   cache_map           cache;

   struct iterator_impl {
      friend chain_kv::view;
      friend iterator;

      chain_kv::view&                    view;
      bytes                              prefix;
      std::unique_ptr<rocksdb::Iterator> rocks_it;
      cache_map::iterator                cache_it;

      iterator_impl(chain_kv::view& view, bytes prefix)
          : view{ view }, prefix{ std::move(prefix) }, rocks_it{ view.db.rdb->NewIterator(rocksdb::ReadOptions()) },
            cache_it{ view.cache.end() } {}

      iterator_impl(const iterator_impl&) = delete;
      iterator_impl& operator=(const iterator_impl&) = delete;

      void rocks_verify_prefix() {
         if (!rocks_it->Valid())
            return;
         auto k = rocks_it->key();
         if (k.size() >= prefix.size() && !memcmp(k.data(), prefix.data(), prefix.size()))
            return;
         rocks_it->SeekToLast();
         if (rocks_it->Valid())
            rocks_it->Next();
      }

      void changed_verify_prefix() {
         if (cache_it == view.cache.end())
            return;
         auto& k = cache_it->first;
         if (k.size() >= prefix.size() && !memcmp(k.data(), prefix.data(), prefix.size()))
            return;
         cache_it = view.cache.end();
      }

      void move_to_begin() {
         rocks_it->Seek({ prefix.data(), prefix.size() });
         rocks_verify_prefix();
         cache_it = view.cache.lower_bound(prefix);
         changed_verify_prefix();
      }

      void move_to_end() {
         rocks_it->SeekToLast();
         if (rocks_it->Valid())
            rocks_it->Next();
         cache_it = view.cache.end();
      }

      void lower_bound(const char* key, size_t size) {
         if (size < prefix.size() || memcmp(key, prefix.data(), prefix.size()))
            throw exception("lower_bound: prefix doesn't match");
         rocks_it->Seek({ key, size });
         rocks_verify_prefix();
         cache_it = view.cache.lower_bound({ key, key + size });
         changed_verify_prefix();
      }

      std::optional<key_value> get_kv() {
         auto r   = deref_rocks_it();
         auto c   = deref_change_it();
         auto min = key_min(r, c);
         if (min) {
            if (min->present)
               return key_value{ min->key, min->value };
            move_to_end(); // invalidate iterator since it is at a removed element
         }
         return {};
      }

      bool is_end() { return !get_kv(); }

      iterator_impl& operator++() {
         auto r   = deref_rocks_it();
         auto c   = deref_change_it();
         auto cmp = compare_key(r, c);
         do {
            if (cmp < 0) {
               rocks_it->Next();
            } else if (cmp > 0) {
               ++cache_it;
            } else if (r && c) {
               rocks_it->Next();
               ++cache_it;
            }
            r   = deref_rocks_it();
            c   = deref_change_it();
            cmp = compare_key(r, c);
         } while (cmp > 0 && !c->present);
         rocks_verify_prefix();
         changed_verify_prefix();
         return *this;
      }

      std::optional<key_present_value> deref_rocks_it() {
         if (rocks_it->Valid())
            return { { rocks_it->key(), true, rocks_it->value() } };
         else
            return {};
      }

      std::optional<key_present_value> deref_change_it() {
         if (cache_it != view.cache.end()) {
            if (cache_it->second.current_value)
               return { { to_slice(cache_it->first), true, to_slice(*cache_it->second.current_value) } };
            else
               return { { to_slice(cache_it->first), false, {} } };
         } else {
            return {};
         }
      }
   }; // iterator_impl

 public:
   class iterator {
      friend view;

    private:
      std::unique_ptr<iterator_impl> impl;

    public:
      iterator(view& view, bytes prefix) : impl{ std::make_unique<iterator_impl>(view, std::move(prefix)) } {}

      iterator(const iterator&) = delete;
      iterator(iterator&&)      = default;

      iterator& operator=(const iterator&) = delete;
      iterator& operator=(iterator&&) = default;

      friend int  compare(const iterator& a, const iterator& b) { return compare_key(a.get_kv(), b.get_kv()); }
      friend bool operator==(const iterator& a, const iterator& b) { return compare(a, b) == 0; }
      friend bool operator<(const iterator& a, const iterator& b) { return compare(a, b) < 0; }

      iterator& operator++() {
         if (impl)
            ++*impl;
         else
            throw exception("kv iterator is not initialized");
         return *this;
      }

      void move_to_begin() {
         if (impl)
            impl->move_to_begin();
         else
            throw exception("kv iterator is not initialized");
      }

      void move_to_end() {
         if (impl)
            impl->move_to_end();
         else
            throw exception("kv iterator is not initialized");
      }

      void lower_bound(const char* key, size_t size) {
         if (impl)
            return impl->lower_bound(key, size);
         else
            throw exception("kv iterator is not initialized");
      }

      void lower_bound(const bytes& key) { return lower_bound(key.data(), key.size()); }

      bool is_end() const { return !impl || impl->is_end(); }

      std::optional<key_value> get_kv() const {
         if (impl)
            return impl->get_kv();
         else
            return {};
      }
   };

   view(database& db) : db{ db } {}

   void discard_changes() {
      write_batch.Clear();
      cache.clear();
   }

   void write_changes() {
      db.write(write_batch);
      discard_changes();
   }

   bool get(rocksdb::Slice k, bytes& dest) {
      bytes    view_prefix;  // !!! temp
      uint64_t contract = 0; // !!! temp

      std::vector<char> adjusted_key;
      adjusted_key.reserve(view_prefix.size() + sizeof(contract) + k.size());
      adjusted_key.insert(adjusted_key.end(), view_prefix.begin(), view_prefix.end());
      append_key(adjusted_key, contract);
      adjusted_key.insert(adjusted_key.end(), k.data(), k.data() + k.size());

      auto it = cache.find(adjusted_key);
      if (it != cache.end()) {
         if (it->second.current_value)
            dest = *it->second.current_value;
         else
            dest.clear();
         return it->second.current_value.has_value();
      }

      rocksdb::PinnableSlice v;
      auto stat = db.rdb->Get(rocksdb::ReadOptions(), db.rdb->DefaultColumnFamily(), to_slice(adjusted_key), &v);
      if (stat.IsNotFound()) {
         dest.clear();
         return false;
      }
      check(stat, "get: ");
      dest.assign(v.data(), v.data() + v.size());

      cache[adjusted_key].current_value = dest;

      return true;
   }

   void set(rocksdb::Slice k, rocksdb::Slice v) {
      // !!! prefix
      // !!! db, contract
      write_batch.Put(k, v);
      cache[{ k.data(), k.data() + k.size() }] = { bytes{ v.data(), v.data() + v.size() } };
   }

   void erase(rocksdb::Slice k) {
      // !!! prefix
      // !!! db, contract
      // !!! iterator invalidation rule change
      write_batch.Delete(k);
      cache[{ k.data(), k.data() + k.size() }] = { std::nullopt };
   }
}; // view

} // namespace chain_kv
