#include "chain_kv_tests.hpp"
#include <boost/filesystem.hpp>

using chain_kv::bytes;
using chain_kv::to_slice;

BOOST_AUTO_TEST_SUITE(undoer_tests)

void undo_tests(bool reload_undoer) {
   boost::filesystem::remove_all("test-db");
   chain_kv::database                db{ "test-db", true };
   std::unique_ptr<chain_kv::undoer> undoer;

   auto reload = [&] {
      if (!undoer || reload_undoer)
         undoer = std::make_unique<chain_kv::undoer>(db, bytes{ 0x10 });
   };
   reload();

   KV_REQUIRE_EXCEPTION(undoer->undo(), "nothing to undo");
   BOOST_REQUIRE_EQUAL(undoer->revision(), 0);
   {
      chain_kv::write_session session{ db };
      session.set({ 0x20, 0x00 }, to_slice({}));
      session.set({ 0x20, 0x02 }, to_slice({ 0x50 }));
      session.set({ 0x20, 0x01 }, to_slice({ 0x40 }));
      session.erase({ 0x20, 0x02 });
      session.set({ 0x20, 0x03 }, to_slice({ 0x60 }));
      session.set({ 0x20, 0x01 }, to_slice({ 0x50 }));
      session.write_changes(*undoer);
   }
   KV_REQUIRE_EXCEPTION(undoer->undo(), "nothing to undo");
   BOOST_REQUIRE_EQUAL(undoer->revision(), 0);
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x10, (char)0x80 }), (kv_values{})); // no undo segments
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x00 }, {} },
                                                    { { 0x20, 0x01 }, { 0x50 } },
                                                    { { 0x20, 0x03 }, { 0x60 } },
                                              } }));

   reload();
   undoer->push();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 1);
   reload();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 1);
   {
      chain_kv::write_session session{ db };
      session.erase({ 0x20, 0x01 });
      session.set({ 0x20, 0x00 }, to_slice({ 0x70 }));
      session.write_changes(*undoer);
   }
   BOOST_REQUIRE_NE(get_all(db, { 0x10, (char)0x80 }), (kv_values{})); // has undo segments
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x00 }, { 0x70 } },
                                                    { { 0x20, 0x03 }, { 0x60 } },
                                              } }));

   BOOST_REQUIRE_EQUAL(undoer->revision(), 1);
   reload();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 1);
   KV_REQUIRE_EXCEPTION(undoer->set_revision(2), "cannot set revision while there is an existing undo stack");
   undoer->undo();
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x10, (char)0x80 }), (kv_values{})); // no undo segments
   BOOST_REQUIRE_EQUAL(undoer->revision(), 0);
   reload();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 0);
   undoer->set_revision(10);
   BOOST_REQUIRE_EQUAL(undoer->revision(), 10);
   reload();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 10);

   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x00 }, {} },
                                                    { { 0x20, 0x01 }, { 0x50 } },
                                                    { { 0x20, 0x03 }, { 0x60 } },
                                              } }));

   {
      chain_kv::write_session session{ db };
      session.erase({ 0x20, 0x01 });
      session.set({ 0x20, 0x00 }, to_slice({ 0x70 }));
      session.write_changes(*undoer);
   }
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x10, (char)0x80 }), (kv_values{})); // no undo segments
   reload();
   undoer->push();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 11);
   reload();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 11);
   KV_REQUIRE_EXCEPTION(undoer->set_revision(12), "cannot set revision while there is an existing undo stack");
   KV_REQUIRE_EXCEPTION(undoer->squash(), "nothing to squash");
   undoer->commit(0);
   BOOST_REQUIRE_EQUAL(undoer->revision(), 11);
   KV_REQUIRE_EXCEPTION(undoer->set_revision(12), "cannot set revision while there is an existing undo stack");
   KV_REQUIRE_EXCEPTION(undoer->squash(), "nothing to squash");
   undoer->commit(11);
   BOOST_REQUIRE_EQUAL(undoer->revision(), 11);
   reload();
   KV_REQUIRE_EXCEPTION(undoer->set_revision(9), "revision cannot decrease");
   undoer->set_revision(12);
   BOOST_REQUIRE_EQUAL(undoer->revision(), 12);
   reload();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 12);

   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x00 }, { 0x70 } },
                                                    { { 0x20, 0x03 }, { 0x60 } },
                                              } }));

} // undo_tests()

BOOST_AUTO_TEST_CASE(test_undo) {
   undo_tests(false);
   undo_tests(true);
}

BOOST_AUTO_TEST_SUITE_END();
