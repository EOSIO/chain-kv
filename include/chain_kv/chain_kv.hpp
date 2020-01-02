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
auto append_key(bytes& dest, T value) -> std::enable_if_t<std::is_unsigned_v<T>, void> {
   char buf[sizeof(value)];
   memcpy(buf, &value, sizeof(value));
   std::reverse(std::begin(buf), std::end(buf));
   dest.insert(dest.end(), std::begin(buf), std::end(buf));
}

template <typename T>
bytes prefix_key(const bytes& prefix, uint64_t contract, const T& key) {
   bytes result;
   result.reserve(prefix.size() + sizeof(contract) + key.size());
   result.insert(result.end(), prefix.begin(), prefix.end());
   append_key(result, contract);
   result.insert(result.end(), key.data(), key.data() + key.size());
   return result;
}

struct database {
   std::unique_ptr<rocksdb::DB> rdb;

   database(const char* db_path, bool create_if_missing, std::optional<uint32_t> threads,
            std::optional<uint32_t> max_open_files) {
      rocksdb::Options options;
      options.create_if_missing                    = create_if_missing;
      options.level_compaction_dynamic_level_bytes = true;
      options.bytes_per_sync                       = 1048576;

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

      rocksdb::DB* p;
      check(rocksdb::DB::Open(options, db_path, &p), "rocksdb::DB::Open: ");
      rdb.reset(p);

      // Sentinals with keys 0x00 and 0xff simplify iteration logic.
      // Views have prefixes which must start with a byte within the range 0x01 - 0xfe.
      rocksdb::WriteBatch batch;
      bool                modified       = false;
      auto                write_sentinal = [&](const bytes& k) {
         rocksdb::PinnableSlice v;
         auto                   stat = rdb->Get(rocksdb::ReadOptions(), rdb->DefaultColumnFamily(), to_slice(k), &v);
         if (stat.IsNotFound()) {
            check(batch.Put(to_slice(k), {}), "put: ");
            modified = true;
         } else {
            check(stat, "get: ");
         }
      };
      write_sentinal({ 0x00 });
      write_sentinal({ (char)0xff });
      if (modified)
         write(batch);
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

inline int compare_key(const std::optional<key_value>& a, const std::optional<key_value>& b) {
   if (!a && !b)
      return 0;
   else if (!a && b)
      return 1;
   else if (a && !b)
      return -1;
   else
      return compare_blob(a->key, b->key);
}

struct cached_value {
   uint64_t             num_erases    = 0;
   std::optional<bytes> current_value = {};
};

using cache_map = std::map<bytes, cached_value, less_blob>;

struct write_session {
   database&           db;
   rocksdb::WriteBatch write_batch;
   cache_map           cache;

   write_session(database& db) : db{ db } {}

   void set(bytes&& k, const rocksdb::Slice& v) {
      check(write_batch.Put(to_slice(k), v), "set: ");
      cache[std::move(k)].current_value = bytes{ v.data(), v.data() + v.size() };
   }

   void erase(bytes&& k) {
      check(write_batch.Delete(to_slice(k)), "erase: ");
      auto& cached = cache[std::move(k)];
      ++cached.num_erases;
      cached.current_value = { std::nullopt };
   }

   cache_map::iterator fill_cache(const rocksdb::Slice& k, const rocksdb::Slice& v) {
      cache_map::iterator it;
      bool                b;
      it = cache.find(k);
      if (it != cache.end())
         return it;
      std::tie(it, b) =
            cache.insert(cache_map::value_type{ bytes{ k.data(), k.data() + k.size() }, //
                                                cached_value{ 0, bytes{ v.data(), v.data() + v.size() } } });
      return it;
   }

   void write_changes() { db.write(write_batch); }
};

class view {
 public:
   class iterator;

   write_session& write_session;
   const bytes    prefix;

 private:
   struct iterator_impl {
      friend chain_kv::view;
      friend iterator;

      chain_kv::view&                    view;
      bytes                              prefix;
      size_t                             hidden_prefix_size;
      bytes                              next_prefix;
      cache_map::iterator                cache_it;
      uint64_t                           cache_it_num_erases = 0;
      std::unique_ptr<rocksdb::Iterator> rocks_it;

      iterator_impl(chain_kv::view& view, uint64_t contract, const rocksdb::Slice& prefix)
          : view{ view },                                                              //
            prefix{ prefix_key(view.prefix, contract, prefix) },                       //
            hidden_prefix_size{ view.prefix.size() + sizeof(contract) },               //
            rocks_it{ view.write_session.db.rdb->NewIterator(rocksdb::ReadOptions()) } //
      {
         next_prefix = this->prefix;
         while (!next_prefix.empty()) {
            if (++next_prefix.back())
               break;
            next_prefix.pop_back();
         }

         rocks_it->Seek(to_slice(this->prefix));
         check(rocks_it->status(), "seek: ");
         view.write_session.fill_cache(rocks_it->key(), rocks_it->value());
         rocks_it->Prev();
         check(rocks_it->status(), "prev: ");
         view.write_session.fill_cache(rocks_it->key(), rocks_it->value());
         rocks_it->Seek(to_slice(next_prefix));
         check(rocks_it->status(), "seek: ");
         view.write_session.fill_cache(rocks_it->key(), rocks_it->value());

         move_to_end();
      }

      iterator_impl(const iterator_impl&) = delete;
      iterator_impl& operator=(const iterator_impl&) = delete;

      void move_to_begin() { lower_bound_full_key(prefix); }

      void move_to_end() { cache_it = view.write_session.cache.end(); }

      void lower_bound(const char* key, size_t size) {
         auto x = compare_blob(rocksdb::Slice{ key, size }, rocksdb::Slice{ prefix.data() + hidden_prefix_size,
                                                                            prefix.size() - hidden_prefix_size });
         if (x < 0) {
            key  = prefix.data() + hidden_prefix_size;
            size = prefix.size() - hidden_prefix_size;
         }

         bytes full_key;
         full_key.reserve(hidden_prefix_size + size);
         full_key.insert(full_key.end(), prefix.data(), prefix.data() + hidden_prefix_size);
         full_key.insert(full_key.end(), key, key + size);
         lower_bound_full_key(full_key);
      }

      void lower_bound_full_key(const bytes& full_key) {
         rocks_it->Seek(to_slice(full_key));
         check(rocks_it->status(), "seek: ");
         cache_it = view.write_session.fill_cache(rocks_it->key(), rocks_it->value());
         if (compare_blob(cache_it->first, to_slice(full_key)))
            cache_it = view.write_session.cache.lower_bound(full_key);
         while (!cache_it->second.current_value) {
            while (compare_blob(rocks_it->key(), cache_it->first) <= 0) {
               rocks_it->Next();
               check(rocks_it->status(), "next: ");
               view.write_session.fill_cache(rocks_it->key(), rocks_it->value());
            }
            ++cache_it;
         }
         if (compare_blob(cache_it->first, next_prefix) >= 0)
            cache_it = view.write_session.cache.end();
         else
            cache_it_num_erases = cache_it->second.num_erases;
      }

      std::optional<key_value> get_kv() {
         if (cache_it == view.write_session.cache.end())
            return {};
         if (cache_it_num_erases != cache_it->second.num_erases)
            throw exception("kv iterator is at an erased value");
         return key_value{ rocksdb::Slice{ cache_it->first.data() + hidden_prefix_size,
                                           cache_it->first.size() - hidden_prefix_size },
                           to_slice(*cache_it->second.current_value) };
      }

      bool is_end() { return cache_it == view.write_session.cache.end(); }

      bool is_valid() {
         return cache_it != view.write_session.cache.end() && cache_it_num_erases == cache_it->second.num_erases;
      }

      iterator_impl& operator++() {
         if (cache_it == view.write_session.cache.end()) {
            move_to_begin();
            return *this;
         } else if (cache_it_num_erases != cache_it->second.num_erases)
            throw exception("kv iterator is at an erased value");
         do {
            while (compare_blob(rocks_it->key(), cache_it->first) <= 0) {
               rocks_it->Next();
               check(rocks_it->status(), "next: ");
               view.write_session.fill_cache(rocks_it->key(), rocks_it->value());
            }
            ++cache_it;
         } while (!cache_it->second.current_value);
         if (compare_blob(cache_it->first, next_prefix) >= 0)
            cache_it = view.write_session.cache.end();
         else
            cache_it_num_erases = cache_it->second.num_erases;
         return *this;
      }

      iterator_impl& operator--() {
         if (cache_it == view.write_session.cache.end()) {
            rocks_it->Seek(to_slice(next_prefix));
            check(rocks_it->status(), "seek: ");
            cache_it = view.write_session.fill_cache(rocks_it->key(), rocks_it->value());
         } else if (cache_it_num_erases != cache_it->second.num_erases)
            throw exception("kv iterator is at an erased value");
         do {
            while (compare_blob(rocks_it->key(), cache_it->first) >= 0) {
               rocks_it->Prev();
               check(rocks_it->status(), "prev: ");
               view.write_session.fill_cache(rocks_it->key(), rocks_it->value());
            }
            --cache_it;
         } while (!cache_it->second.current_value);
         if (compare_blob(cache_it->first, prefix) < 0)
            cache_it = view.write_session.cache.end();
         else
            cache_it_num_erases = cache_it->second.num_erases;
         return *this;
      }
   }; // iterator_impl

 public:
   class iterator {
      friend view;

    private:
      std::unique_ptr<iterator_impl> impl;

      void check_initialized() const {
         if (!impl)
            throw exception("kv iterator is not initialized");
      }

    public:
      iterator(view& view, uint64_t contract, const rocksdb::Slice& prefix)
          : impl{ std::make_unique<iterator_impl>(view, contract, std::move(prefix)) } {}

      iterator(const iterator&) = delete;
      iterator(iterator&&)      = default;

      iterator& operator=(const iterator&) = delete;
      iterator& operator=(iterator&&) = default;

      friend int  compare(const iterator& a, const iterator& b) { return compare_key(a.get_kv(), b.get_kv()); }
      friend bool operator==(const iterator& a, const iterator& b) { return compare(a, b) == 0; }
      friend bool operator!=(const iterator& a, const iterator& b) { return compare(a, b) != 0; }
      friend bool operator<(const iterator& a, const iterator& b) { return compare(a, b) < 0; }
      friend bool operator<=(const iterator& a, const iterator& b) { return compare(a, b) <= 0; }
      friend bool operator>(const iterator& a, const iterator& b) { return compare(a, b) > 0; }
      friend bool operator>=(const iterator& a, const iterator& b) { return compare(a, b) >= 0; }

      iterator& operator++() {
         check_initialized();
         ++*impl;
         return *this;
      }

      iterator& operator--() {
         check_initialized();
         --*impl;
         return *this;
      }

      void move_to_begin() {
         check_initialized();
         impl->move_to_begin();
      }

      void move_to_end() {
         check_initialized();
         impl->move_to_end();
      }

      void lower_bound(const char* key, size_t size) {
         check_initialized();
         impl->lower_bound(key, size);
      }

      void lower_bound(const bytes& key) { lower_bound(key.data(), key.size()); }

      bool is_end() const {
         check_initialized();
         return impl->is_end();
      }

      bool is_valid() const {
         check_initialized();
         return impl->is_valid();
      }

      std::optional<key_value> get_kv() const {
         check_initialized();
         return impl->get_kv();
      }
   };

   view(struct write_session& write_session, bytes prefix)
       : write_session{ write_session }, prefix{ std::move(prefix) } {
      if (this->prefix.empty())
         throw exception("kv view may not have empty prefix");

      // Sentinals reserve 0x00 and 0xff. This keeps rocksdb iterators from going
      // invalid during iteration.
      if (this->prefix[0] == 0x00 || this->prefix[0] == (char)0xff)
         throw exception("view may not have a prefix which begins with 0x00 or 0xff");
   }

   bool get(uint64_t contract, const rocksdb::Slice& k, bytes& dest) {
      auto prefixed = prefix_key(prefix, contract, k);

      auto it = write_session.cache.find(prefixed);
      if (it != write_session.cache.end()) {
         if (it->second.current_value)
            dest = *it->second.current_value;
         else
            dest.clear();
         return it->second.current_value.has_value();
      }

      rocksdb::PinnableSlice v;
      auto stat = write_session.db.rdb->Get(rocksdb::ReadOptions(), write_session.db.rdb->DefaultColumnFamily(),
                                            to_slice(prefixed), &v);
      if (stat.IsNotFound()) {
         dest.clear();
         return false;
      }
      check(stat, "get: ");
      dest.assign(v.data(), v.data() + v.size());
      write_session.cache[prefixed].current_value = dest;
      return true;
   }

   void set(uint64_t contract, const rocksdb::Slice& k, const rocksdb::Slice& v) {
      write_session.set(prefix_key(prefix, contract, k), v);
   }

   void erase(uint64_t contract, const rocksdb::Slice& k) { write_session.erase(prefix_key(prefix, contract, k)); }
}; // view

} // namespace chain_kv
