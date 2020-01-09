#pragma once

#include <fc/io/raw.hpp>
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

using bytes = std::vector<char>;

inline rocksdb::Slice to_slice(const bytes& v) { return { v.data(), v.size() }; }

inline bytes to_bytes(const rocksdb::Slice& v) { return { v.data(), v.data() + v.size() }; }

inline std::shared_ptr<bytes> to_shared_bytes(const rocksdb::Slice& v) {
   return std::make_shared<bytes>(v.data(), v.data() + v.size());
}

template <typename Stream>
void pack_bytes(Stream& s, const bytes& b) {
   fc::unsigned_int size(b.size());
   if (size.value != b.size())
      throw exception("bytes is too big");
   fc::raw::pack(s, size);
   s.write(b.data(), b.size());
}

template <typename Stream>
std::pair<const char*, size_t> get_bytes(Stream& s) {
   fc::unsigned_int size;
   fc::raw::unpack(s, size);
   if (size > s.remaining())
      throw exception("bad size for bytes");
   auto data = s.pos();
   s.skip(size);
   return { data, size.value };
}

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

struct less_blob {
   using is_transparent = void;

   template <typename A, typename B>
   bool operator()(const A& a, const B& b) const {
      return compare_blob(a, b) < 0;
   }
};

inline bytes get_next_prefix(const bytes& prefix) {
   bytes next_prefix;
   next_prefix = prefix;
   while (!next_prefix.empty()) {
      if (++next_prefix.back())
         break;
      next_prefix.pop_back();
   }
   return next_prefix;
}

template <typename T>
auto append_key(bytes& dest, T value) -> std::enable_if_t<std::is_unsigned_v<T>, void> {
   char buf[sizeof(value)];
   memcpy(buf, &value, sizeof(value));
   std::reverse(std::begin(buf), std::end(buf));
   dest.insert(dest.end(), std::begin(buf), std::end(buf));
}

template <typename T>
bytes create_full_key(const bytes& prefix, uint64_t contract, const T& key) {
   bytes result;
   result.reserve(prefix.size() + sizeof(contract) + key.size());
   result.insert(result.end(), prefix.begin(), prefix.end());
   append_key(result, contract);
   result.insert(result.end(), key.data(), key.data() + key.size());
   return result;
}

struct database {
   std::unique_ptr<rocksdb::DB> rdb;

   database(const char* db_path, bool create_if_missing, std::optional<uint32_t> threads = {},
            std::optional<uint32_t> max_open_files = {}) {

      rocksdb::Options options;
      options.create_if_missing                    = create_if_missing;
      options.level_compaction_dynamic_level_bytes = true;
      options.bytes_per_sync                       = 1048576;

      if (threads)
         options.IncreaseParallelism(*threads);

      options.OptimizeLevelStyleCompaction(256ull << 20);

      if (max_open_files)
         options.max_open_files = *max_open_files;

      rocksdb::BlockBasedTableOptions table_options;
      table_options.format_version               = 4;
      table_options.index_block_restart_interval = 16;
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));

      rocksdb::DB* p;
      check(rocksdb::DB::Open(options, db_path, &p), "rocksdb::DB::Open: ");
      rdb.reset(p);

      // Sentinels with keys 0x00 and 0xff simplify iteration logic.
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
   // nullopt represents end; everything else is before end
   if (!a && !b)
      return 0;
   else if (!a && b)
      return 1;
   else if (a && !b)
      return -1;
   else
      return compare_blob(a->key, b->key);
}

inline int compare_value(const std::shared_ptr<const bytes>& a, const std::shared_ptr<const bytes>& b) {
   // nullptr represents erased; everything else is after erased
   if (!a && !b)
      return 0;
   else if (!a && b)
      return -1;
   else if (a && !b)
      return 1;
   else
      return compare_blob(*a, *b);
}

using cache_map = std::map<bytes, struct cached_value, less_blob>;

struct cached_value {
   uint64_t                     num_erases       = 0;
   std::shared_ptr<const bytes> orig_value       = {};
   std::shared_ptr<const bytes> current_value    = {};
   bool                         in_change_list   = false;
   cache_map::iterator          change_list_next = {};
};

struct undo_state {
   uint8_t               format_version    = 0;
   int64_t               revision          = 0;
   std::vector<uint64_t> undo_stack        = {}; // Number of undo segments needed to go back each revision
   uint64_t              next_undo_segment = 0;
};

enum class undo_type : uint8_t {
   remove = 0,
   put    = 1,
};

template <typename Stream>
void pack_remove(Stream& s, const bytes& key) {
   s << uint8_t(undo_type::remove);
   pack_bytes(s, key);
}

template <typename Stream>
void pack_put(Stream& s, const bytes& key, const bytes& value) {
   s << uint8_t(undo_type::put);
   pack_bytes(s, key);
   pack_bytes(s, value);
}

struct undo_stack {
   database&  db;
   bytes      undo_prefix;
   bytes      state_prefix;
   bytes      segment_prefix;
   bytes      segment_next_prefix;
   uint64_t   target_segment_size = 64 * 1024 * 1024;
   undo_state state;

   undo_stack(database& db, bytes&& undo_prefix) : db{ db }, undo_prefix{ std::move(undo_prefix) } {
      if (this->undo_prefix.empty())
         throw exception("undo_prefix is empty");

      // Sentinels reserve 0x00 and 0xff. This keeps rocksdb iterators from going
      // invalid during iteration.
      if (this->undo_prefix[0] == 0x00 || this->undo_prefix[0] == (char)0xff)
         throw exception("undo_stack may not have a prefix which begins with 0x00 or 0xff");

      state_prefix = this->undo_prefix;
      state_prefix.push_back(0x00);
      segment_prefix = this->undo_prefix;
      segment_prefix.push_back(0x80);
      segment_next_prefix = get_next_prefix(segment_prefix);

      rocksdb::PinnableSlice v;
      auto stat = db.rdb->Get(rocksdb::ReadOptions(), db.rdb->DefaultColumnFamily(), to_slice(this->state_prefix), &v);
      if (!stat.IsNotFound())
         check(stat, "get: ");
      if (stat.ok()) {
         auto format_version = fc::raw::unpack<uint8_t>(v.data(), v.size());
         if (format_version)
            throw exception("invalid undo format");
         state = fc::raw::unpack<undo_state>(v.data(), v.size());
      }
   }

   int64_t revision() const { return state.revision; }

   void set_revision(uint64_t revision) {
      if (state.undo_stack.size() != 0)
         throw exception("cannot set revision while there is an existing undo stack");
      if (revision > std::numeric_limits<int64_t>::max())
         throw exception("revision to set is too high");
      if (revision < state.revision)
         throw exception("revision cannot decrease");
      state.revision = revision;
      write_state();
   }

   // Create a new entry on the undo stack
   void push() {
      state.undo_stack.push_back(0);
      ++state.revision;
      write_state();
   }

   // Combine the top two states on the undo stack
   void squash() {
      if (state.undo_stack.size() < 2)
         throw exception("nothing to squash");
      auto n = state.undo_stack.back();
      state.undo_stack.pop_back();
      state.undo_stack.back() += n;
      --state.revision;
      write_state();
   }

   // Reset the contents to the state at the top of the undo stack
   void undo() {
      if (state.undo_stack.empty())
         throw exception("nothing to undo");
      rocksdb::WriteBatch batch;

      std::unique_ptr<rocksdb::Iterator> rocks_it{ db.rdb->NewIterator(rocksdb::ReadOptions()) };
      auto                               first = create_segment_key(state.next_undo_segment - state.undo_stack.back());
      rocks_it->Seek(to_slice(segment_next_prefix));
      if (rocks_it->Valid())
         rocks_it->Prev();

      while (rocks_it->Valid()) {
         auto segment_key = rocks_it->key();
         if (compare_blob(segment_key, first) < 0)
            break;
         auto                        segment = rocks_it->value();
         fc::datastream<const char*> ds(segment.data(), segment.size());
         while (ds.remaining()) {
            uint8_t type;
            fc::raw::unpack(ds, type);
            if (type == (uint8_t)undo_type::remove) {
               auto [key, key_size] = get_bytes(ds);
               check(batch.Delete({ key, key_size }), "erase: ");
            } else if (type == (uint8_t)undo_type::put) {
               auto [key, key_size]     = get_bytes(ds);
               auto [value, value_size] = get_bytes(ds);
               check(batch.Put({ key, key_size }, { value, value_size }), "set: ");
            } else {
               throw exception("unknown undo_type");
            }
         }
         check(batch.Delete(segment_key), "erase: ");
         rocks_it->Prev();
      }
      check(rocks_it->status(), "iterate: ");

      state.next_undo_segment -= state.undo_stack.back();
      state.undo_stack.pop_back();
      --state.revision;
      write_state(batch);
      db.write(batch);
   }

   // Discard all undo history prior to revision
   void commit(int64_t revision) {
      revision            = std::min(revision, state.revision);
      auto first_revision = state.revision - state.undo_stack.size();
      if (first_revision < revision) {
         rocksdb::WriteBatch batch;
         state.undo_stack.erase(state.undo_stack.begin(), state.undo_stack.begin() + (revision - first_revision));
         uint64_t keep_undo_segment = state.next_undo_segment;
         for (auto n : state.undo_stack) //
            keep_undo_segment -= n;
         if (keep_undo_segment > 0)
            check(batch.DeleteRange(to_slice(create_segment_key(0)),
                                    to_slice(create_segment_key(keep_undo_segment - 1))),
                  "delete range:");
         write_state(batch);
         db.write(batch);
      }
   }

   void write_state(rocksdb::WriteBatch& batch) {
      check(batch.Put(to_slice(state_prefix), to_slice(fc::raw::pack(state))), "set: ");
   }

   void write_state() {
      rocksdb::WriteBatch batch;
      write_state(batch);
      db.write(batch);
   }

   bytes create_segment_key(uint64_t segment) {
      bytes key;
      key.reserve(segment_prefix.size() + sizeof(segment));
      key.insert(key.end(), segment_prefix.begin(), segment_prefix.end());
      append_key(key, segment);
      return key;
   }

   void write_changes(cache_map& cache, cache_map::iterator change_list) {
      rocksdb::WriteBatch batch;
      bytes               segment;
      segment.reserve(target_segment_size);

      auto write_segment = [&] {
         if (segment.empty())
            return;
         auto key = create_segment_key(state.next_undo_segment++);
         check(batch.Put(to_slice(key), to_slice(segment)), "set: ");
         ++state.undo_stack.back();
         segment.clear();
      };

      auto append_segment = [&](auto f) {
         fc::datastream<size_t> size_stream;
         f(size_stream);
         if (segment.size() + size_stream.tellp() > target_segment_size)
            write_segment();
         auto orig_size = segment.size();
         segment.resize(segment.size() + size_stream.tellp());
         fc::datastream<char*> ds(segment.data() + orig_size, size_stream.tellp());
         f(ds);
      };

      auto it = change_list;
      while (it != cache.end()) {
         if (compare_value(it->second.orig_value, it->second.current_value)) {
            if (it->second.current_value)
               check(batch.Put(to_slice(it->first), to_slice(*it->second.current_value)), "set: ");
            else
               check(batch.Delete(to_slice(it->first)), "erase: ");

            bool first_in_segment = segment.empty();
            if (!state.undo_stack.empty()) {
               if (it->second.orig_value) {
                  append_segment([&](auto& stream) { pack_put(stream, it->first, *it->second.orig_value); });
               } else {
                  append_segment([&](auto& stream) { pack_remove(stream, it->first); });
               }
            }
         }
         it = it->second.change_list_next;
      }

      write_segment();
      write_state(batch);
      db.write(batch);
   } // write()
};   // undo_stack

struct write_session {
   database&           db;
   cache_map           cache;
   cache_map::iterator change_list = cache.end();

   write_session(database& db) : db{ db } {}

   void changed(cache_map::iterator it) {
      if (it->second.in_change_list)
         return;
      it->second.in_change_list   = true;
      it->second.change_list_next = change_list;
      change_list                 = it;
   }

   bool get(bytes&& k, bytes& dest) {
      auto it = cache.find(k);
      if (it != cache.end()) {
         if (it->second.current_value)
            dest = *it->second.current_value;
         else
            dest.clear();
         return it->second.current_value != nullptr;
      }

      rocksdb::PinnableSlice v;
      auto                   stat = db.rdb->Get(rocksdb::ReadOptions(), db.rdb->DefaultColumnFamily(), to_slice(k), &v);
      if (stat.IsNotFound()) {
         dest.clear();
         return false;
      }
      check(stat, "get: ");

      auto value          = to_shared_bytes(v);
      cache[std::move(k)] = cached_value{ 0, value, value };
      dest                = *value;
      return true;
   }

   void set(bytes&& k, const rocksdb::Slice& v) {
      auto it = cache.find(k);
      if (it != cache.end()) {
         if (!it->second.current_value || compare_blob(*it->second.current_value, v)) {
            it->second.current_value = to_shared_bytes(v);
            changed(it);
         }
         return;
      }

      rocksdb::PinnableSlice orig_v;
      auto stat = db.rdb->Get(rocksdb::ReadOptions(), db.rdb->DefaultColumnFamily(), to_slice(k), &orig_v);
      if (stat.IsNotFound()) {
         auto [it, b] =
               cache.insert(cache_map::value_type{ std::move(k), cached_value{ 0, nullptr, to_shared_bytes(v) } });
         changed(it);
         return;
      }

      check(stat, "get: ");
      if (compare_blob(v, orig_v)) {
         auto [it, b] = cache.insert(
               cache_map::value_type{ std::move(k), cached_value{ 0, to_shared_bytes(orig_v), to_shared_bytes(v) } });
         changed(it);
      } else {
         auto value          = to_shared_bytes(orig_v);
         cache[std::move(k)] = cached_value{ 0, value, value };
      }
   }

   void erase(bytes&& k) {
      {
         auto it = cache.find(k);
         if (it != cache.end()) {
            if (it->second.current_value) {
               ++it->second.num_erases;
               it->second.current_value = nullptr;
               changed(it);
            }
            return;
         }
      }

      rocksdb::PinnableSlice orig_v;
      auto stat = db.rdb->Get(rocksdb::ReadOptions(), db.rdb->DefaultColumnFamily(), to_slice(k), &orig_v);
      if (stat.IsNotFound()) {
         cache[std::move(k)] = cached_value{ 0, nullptr, nullptr };
         return;
      }

      check(stat, "get: ");
      auto [it, b] =
            cache.insert(cache_map::value_type{ std::move(k), cached_value{ 1, to_shared_bytes(orig_v), nullptr } });
      changed(it);
   }

   cache_map::iterator fill_cache(const rocksdb::Slice& k, const rocksdb::Slice& v) {
      cache_map::iterator it;
      bool                b;
      it = cache.find(k);
      if (it != cache.end())
         return it;
      auto value      = to_shared_bytes(v);
      std::tie(it, b) = cache.insert(cache_map::value_type{ to_bytes(k), cached_value{ 0, value, value } });
      return it;
   }

   void write_changes(undo_stack& u) { u.write_changes(cache, change_list); }
}; // write_session

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
            prefix{ create_full_key(view.prefix, contract, prefix) },                  //
            hidden_prefix_size{ view.prefix.size() + sizeof(contract) },               //
            rocks_it{ view.write_session.db.rdb->NewIterator(rocksdb::ReadOptions()) } //
      {
         next_prefix = get_next_prefix(this->prefix);

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
            if (compare_blob(cache_it->first, to_slice(next_prefix)))
               cache_it = view.write_session.cache.lower_bound(next_prefix);
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

      // Sentinels reserve 0x00 and 0xff. This keeps rocksdb iterators from going
      // invalid during iteration. This also allows get_next_prefix() to function correctly.
      if (this->prefix[0] == 0x00 || this->prefix[0] == (char)0xff)
         throw exception("view may not have a prefix which begins with 0x00 or 0xff");
   }

   bool get(uint64_t contract, const rocksdb::Slice& k, bytes& dest) {
      return write_session.get(create_full_key(prefix, contract, k), dest);
   }

   void set(uint64_t contract, const rocksdb::Slice& k, const rocksdb::Slice& v) {
      write_session.set(create_full_key(prefix, contract, k), v);
   }

   void erase(uint64_t contract, const rocksdb::Slice& k) { write_session.erase(create_full_key(prefix, contract, k)); }
}; // view

} // namespace chain_kv

FC_REFLECT(chain_kv::undo_state, (format_version)(revision)(undo_stack)(next_undo_segment))
