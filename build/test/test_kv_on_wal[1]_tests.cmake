add_test([=[FileTest.Put]=]  /home/knight/cpp_pros/little_kv/build/test/test_kv_on_wal [==[--gtest_filter=FileTest.Put]==] --gtest_also_run_disabled_tests)
set_tests_properties([=[FileTest.Put]=]  PROPERTIES WORKING_DIRECTORY /home/knight/cpp_pros/little_kv/build/test SKIP_REGULAR_EXPRESSION [==[\[  SKIPPED \]]==])
set(  test_kv_on_wal_TESTS FileTest.Put)
