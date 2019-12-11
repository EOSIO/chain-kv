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

class view {
 public:
   database& db;

   class iterator;
   using bytes = std::vector<char>;

   struct key_value {
      rocksdb::Slice key   = {};
      rocksdb::Slice value = {};
   };

   struct key_present_value {
      rocksdb::Slice key     = {};
      bool           present = {};
      rocksdb::Slice value   = {};
   };

   struct present_value {
      bool  present = {};
      bytes value   = {};
   };

 private:
   struct vector_compare {
      bool operator()(const std::vector<char>& a, const std::vector<char>& b) const {
         return compare_blob(to_slice(a), to_slice(b));
      }
   };

   template <typename T>
   static int compare_key(const std::optional<T>& a, const std::optional<T>& b) {
      if (!a && !b)
         return 0;
      else if (!a && b)
         return 1;
      else if (a && !b)
         return -1;
      else
         return compare_blob(a->key, b->key);
   }

   static const std::optional<key_present_value>& key_min(const std::optional<key_present_value>& a,
                                                          const std::optional<key_present_value>& b) {
      auto cmp = compare_key(a, b);
      if (cmp <= 0)
         return a;
      else
         return b;
   }

   using change_map = std::map<bytes, present_value, vector_compare>;

   rocksdb::WriteBatch write_batch;
   change_map          changes;

   struct iterator_impl {
      friend chain_kv::view;
      friend iterator;

      chain_kv::view&                    view;
      std::vector<char>                  prefix;
      std::unique_ptr<rocksdb::Iterator> rocks_it;
      change_map::iterator               change_it;

      iterator_impl(chain_kv::view& view, std::vector<char> prefix)
          : view{ view }, prefix{ std::move(prefix) }, rocks_it{ view.db.rdb->NewIterator(rocksdb::ReadOptions()) },
            change_it{ view.changes.end() } {}

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
         if (change_it == view.changes.end())
            return;
         auto& k = change_it->first;
         if (k.size() >= prefix.size() && !memcmp(k.data(), prefix.data(), prefix.size()))
            return;
         change_it = view.changes.end();
      }

      void move_to_begin() {
         rocks_it->Seek({ prefix.data(), prefix.size() });
         rocks_verify_prefix();
         change_it = view.changes.lower_bound(prefix);
         changed_verify_prefix();
      }

      void move_to_end() {
         rocks_it->SeekToLast();
         if (rocks_it->Valid())
            rocks_it->Next();
         change_it = view.changes.end();
      }

      void lower_bound(const char* key, size_t size) {
         if (size < prefix.size() || memcmp(key, prefix.data(), prefix.size()))
            throw exception("lower_bound: prefix doesn't match");
         rocks_it->Seek({ key, size });
         rocks_verify_prefix();
         change_it = view.changes.lower_bound({ key, key + size });
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
               ++change_it;
            } else if (r && c) {
               rocks_it->Next();
               ++change_it;
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
         if (change_it != view.changes.end())
            return { { to_slice(change_it->first), change_it->second.present, to_slice(change_it->second.value) } };
         else
            return {};
      }
   }; // iterator_impl

 public:
   class iterator {
      friend view;

    private:
      std::unique_ptr<iterator_impl> impl;

    public:
      iterator(view& view, std::vector<char> prefix)
          : impl{ std::make_unique<iterator_impl>(view, std::move(prefix)) } {}

      iterator(const iterator&) = delete;
      iterator(iterator&&)      = default;

      iterator& operator=(const iterator&) = delete;
      iterator& operator=(iterator&&) = default;

      friend int  compare(const iterator& a, const iterator& b) { return view::compare_key(a.get_kv(), b.get_kv()); }
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

      void lower_bound(const std::vector<char>& key) { return lower_bound(key.data(), key.size()); }

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
      changes.clear();
   }

   void write_changes() {
      db.write(write_batch);
      discard_changes();
   }

   bool get(rocksdb::Slice k, std::vector<char>& dest) {
      // !!! prefix
      // !!! db, contract
      rocksdb::PinnableSlice v;
      auto                   stat = db.rdb->Get(rocksdb::ReadOptions(), db.rdb->DefaultColumnFamily(), k, &v);
      if (stat.IsNotFound()) {
         dest.clear();
         return false;
      }
      check(stat, "get: ");
      dest.assign(v.data(), v.data() + v.size());
      return true;
   }

   void set(rocksdb::Slice k, rocksdb::Slice v) {
      // !!! prefix
      // !!! db, contract
      write_batch.Put(k, v);
      changes[{ k.data(), k.data() + k.size() }] = { true, { v.data(), v.data() + v.size() } };
   }

   void erase(rocksdb::Slice k) {
      // !!! prefix
      // !!! db, contract
      // !!! iterator invalidation rule change
      write_batch.Delete(k);
      changes[{ k.data(), k.data() + k.size() }] = { false, {} };
   }
}; // view

} // namespace chain_kv
