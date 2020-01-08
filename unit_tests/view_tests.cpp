#include "chain_kv_tests.hpp"
#include <boost/filesystem.hpp>

using chain_kv::bytes;
using chain_kv::to_slice;

BOOST_AUTO_TEST_SUITE(view_tests)

void view_test(bool reload_session) {
   boost::filesystem::remove_all("test-write-session-db");
   chain_kv::database                       db{ "test-write-session-db", true };
   chain_kv::undo_stack                     undo_stack{ db, { 0x10 } };
   std::unique_ptr<chain_kv::write_session> session;
   std::unique_ptr<chain_kv::view>          view;

   auto reload = [&] {
      if (session && reload_session) {
         session->write_changes(undo_stack);
         view    = nullptr;
         session = nullptr;
      }
      if (!session)
         session = std::make_unique<chain_kv::write_session>(db);
      if (!view)
         view = std::make_unique<chain_kv::view>(*session, bytes{ 0x70 });
   };
   reload();

   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x1234), (kv_values{}));
   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x1234), get_matching2(*view, 0x1234));
   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x5678), (kv_values{}));
   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x5678), get_matching2(*view, 0x5678));
   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x9abc), (kv_values{}));
   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x9abc), get_matching2(*view, 0x9abc));

   view->set(0x1234, to_slice({ 0x30, 0x40 }), to_slice({ 0x50, 0x60 }));
   view->set(0x5678, to_slice({ 0x30, 0x41 }), to_slice({ 0x51, 0x61 }));
   view->set(0x9abc, to_slice({ 0x30, 0x42 }), to_slice({ 0x52, 0x62 }));
   reload();

   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x1234), (kv_values{ {
                                                          { { 0x30, 0x40 }, { 0x50, 0x60 } },
                                                    } }));
   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x1234), get_matching2(*view, 0x1234));
   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x5678), (kv_values{ {
                                                          { { 0x30, 0x41 }, { 0x51, 0x61 } },
                                                    } }));
   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x5678), get_matching2(*view, 0x5678));
   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x9abc), (kv_values{ {
                                                          { { 0x30, 0x42 }, { 0x52, 0x62 } },
                                                    } }));
   BOOST_REQUIRE_EQUAL(get_matching(*view, 0x9abc), get_matching2(*view, 0x9abc));
} // view_test()

BOOST_AUTO_TEST_CASE(test_view) {
   view_test(false);
   view_test(true);
}

BOOST_AUTO_TEST_SUITE_END();
