include(ExternalProject)

file(GLOB UNIT_TESTS "*.cpp") # find all unit test suites

add_executable(unit_test ${UNIT_TESTS})
target_link_libraries(unit_test fc rocksdb)
target_include_directories(unit_test PRIVATE ../include ${ROCKSDB_INCLUDE_DIRS})
target_compile_options(unit_test PUBLIC ${ROCKSDB_CFLAGS_OTHER})

enable_testing()

### MARK TEST SUITES FOR EXECUTION ###
foreach(TEST_SUITE ${UNIT_TESTS}) # create an independent target for each test suite
   execute_process(COMMAND bash -c "grep -E 'BOOST_AUTO_TEST_SUITE\\s*[(]' ${TEST_SUITE} | grep -vE '//.*BOOST_AUTO_TEST_SUITE\\s*[(]' | cut -d ')' -f 1 | cut -d '(' -f 2" OUTPUT_VARIABLE SUITE_NAME OUTPUT_STRIP_TRAILING_WHITESPACE) # get the test suite name from the *.cpp file
   if (NOT "" STREQUAL "${SUITE_NAME}") # ignore empty lines
      execute_process(COMMAND bash -c "echo ${SUITE_NAME} | sed -e 's/s$//' | sed -e 's/_test$//'" OUTPUT_VARIABLE TRIMMED_SUITE_NAME OUTPUT_STRIP_TRAILING_WHITESPACE) # trim "_test" or "_tests" from the end of ${SUITE_NAME}
         add_test(NAME ${TRIMMED_SUITE_NAME}_unit_test COMMAND unit_test --run_test=${SUITE_NAME} --report_level=detailed --color_output --catch_system_errors=no)
         # build list of tests to run during coverage testing
         if(ctest_tests)
            string(APPEND ctest_tests "|")
         endif()
         string(APPEND ctest_tests ${TRIMMED_SUITE_NAME}_unit_test)
   endif()
endforeach(TEST_SUITE)
set(ctest_tests "'${ctest_tests}' -j8")
