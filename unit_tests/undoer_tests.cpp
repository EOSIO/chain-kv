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

void squash_tests(bool reload_undoer) {
   boost::filesystem::remove_all("test-db");
   chain_kv::database                db{ "test-db", true };
   std::unique_ptr<chain_kv::undoer> undoer;

   auto reload = [&] {
      if (!undoer || reload_undoer)
         undoer = std::make_unique<chain_kv::undoer>(db, bytes{ 0x10 });
   };
   reload();

   // set 1
   undoer->push();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 1);
   {
      chain_kv::write_session session{ db };
      session.set({ 0x20, 0x01 }, to_slice({ 0x50 }));
      session.set({ 0x20, 0x02 }, to_slice({ 0x60 }));
      session.write_changes(*undoer);
   }
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x01 }, { 0x50 } },
                                                    { { 0x20, 0x02 }, { 0x60 } },
                                              } }));

   // set 2
   undoer->push();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 2);
   {
      chain_kv::write_session session{ db };
      session.erase({ 0x20, 0x01 });
      session.set({ 0x20, 0x02 }, to_slice({ 0x61 }));
      session.set({ 0x20, 0x03 }, to_slice({ 0x70 }));
      session.set({ 0x20, 0x04 }, to_slice({ 0x10 }));
      session.write_changes(*undoer);
   }
   undoer->push();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 3);
   {
      chain_kv::write_session session{ db };
      session.set({ 0x20, 0x01 }, to_slice({ 0x50 }));
      session.set({ 0x20, 0x02 }, to_slice({ 0x62 }));
      session.erase({ 0x20, 0x03 });
      session.set({ 0x20, 0x05 }, to_slice({ 0x05 }));
      session.set({ 0x20, 0x06 }, to_slice({ 0x06 }));
      session.write_changes(*undoer);
   }
   undoer->squash();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 2);
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x01 }, { 0x50 } },
                                                    { { 0x20, 0x02 }, { 0x62 } },
                                                    { { 0x20, 0x04 }, { 0x10 } },
                                                    { { 0x20, 0x05 }, { 0x05 } },
                                                    { { 0x20, 0x06 }, { 0x06 } },
                                              } }));

   // set 3
   undoer->push();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 3);
   {
      chain_kv::write_session session{ db };
      session.set({ 0x20, 0x07 }, to_slice({ 0x07 }));
      session.set({ 0x20, 0x08 }, to_slice({ 0x08 }));
      session.write_changes(*undoer);
   }
   undoer->push();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 4);
   {
      chain_kv::write_session session{ db };
      session.set({ 0x20, 0x09 }, to_slice({ 0x09 }));
      session.set({ 0x20, 0x0a }, to_slice({ 0x0a }));
      session.write_changes(*undoer);
   }
   undoer->push();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 5);
   {
      chain_kv::write_session session{ db };
      session.set({ 0x20, 0x0b }, to_slice({ 0x0b }));
      session.set({ 0x20, 0x0c }, to_slice({ 0x0c }));
      session.write_changes(*undoer);
   }
   undoer->squash();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 4);
   undoer->squash();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 3);
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x01 }, { 0x50 } },
                                                    { { 0x20, 0x02 }, { 0x62 } },
                                                    { { 0x20, 0x04 }, { 0x10 } },
                                                    { { 0x20, 0x05 }, { 0x05 } },
                                                    { { 0x20, 0x06 }, { 0x06 } },
                                                    { { 0x20, 0x07 }, { 0x07 } },
                                                    { { 0x20, 0x08 }, { 0x08 } },
                                                    { { 0x20, 0x09 }, { 0x09 } },
                                                    { { 0x20, 0x0a }, { 0x0a } },
                                                    { { 0x20, 0x0b }, { 0x0b } },
                                                    { { 0x20, 0x0c }, { 0x0c } },
                                              } }));

   // undo set 3
   undoer->undo();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 2);
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x01 }, { 0x50 } },
                                                    { { 0x20, 0x02 }, { 0x62 } },
                                                    { { 0x20, 0x04 }, { 0x10 } },
                                                    { { 0x20, 0x05 }, { 0x05 } },
                                                    { { 0x20, 0x06 }, { 0x06 } },
                                              } }));

   // undo set 2
   undoer->undo();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 1);
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {
                                                    { { 0x20, 0x01 }, { 0x50 } },
                                                    { { 0x20, 0x02 }, { 0x60 } },
                                              } }));

   // undo set 1
   undoer->undo();
   BOOST_REQUIRE_EQUAL(undoer->revision(), 0);
   BOOST_REQUIRE_EQUAL(get_all(db, { 0x20 }), (kv_values{ {} }));
} // squash_tests()

BOOST_AUTO_TEST_CASE(test_undo) {
   undo_tests(false);
   undo_tests(true);
}

BOOST_AUTO_TEST_CASE(test_squash) {
   squash_tests(false);
   squash_tests(true);
}

BOOST_AUTO_TEST_SUITE_END();
