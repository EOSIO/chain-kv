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

   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x00 }, { 0x70 } },
                                                    { { 0x20, 0x03 }, { 0x60 } },
                                              } }));

   reload();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 1);
   undoer->undo();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 0);
   reload();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 0);

   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x00 }, {} },
                                                    { { 0x20, 0x01 }, { 0x50 } },
                                                    { { 0x20, 0x03 }, { 0x60 } },
                                              } }));
} // undo_tests()

BOOST_AUTO_TEST_CASE(test_undo) {
   undo_tests(false);
   undo_tests(true);
}

BOOST_AUTO_TEST_SUITE_END();
