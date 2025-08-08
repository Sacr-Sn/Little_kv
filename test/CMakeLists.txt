enable_testing()
find_package(GTest REQUIRED)
include(GoogleTest)

# add_executable(test_kv_on_memory test_memory.cpp)

# target_include_directories(test_kv_on_memory PRIVATE ../src/kv_on_memory/headers)
# target_link_libraries(test_kv_on_memory PRIVATE kv_on_memory GTest::GTest GTest::Main)

# gtest_discover_tests(test_kv_on_memory)

# ==============================================

# add_executable(test_kv_on_file test_file.cpp)

# target_include_directories(test_kv_on_file PRIVATE ../src/kv_on_file/headers)
# target_link_libraries(test_kv_on_file PRIVATE kv_on_file GTest::GTest GTest::Main)

# gtest_discover_tests(test_kv_on_file)

# ==============================================

# add_executable(test_kv_on_wal test_wal.cpp)

# target_include_directories(test_kv_on_wal PRIVATE ../src/kv_on_wal/headers)
# target_link_libraries(test_kv_on_wal PRIVATE kv_on_wal GTest::GTest GTest::Main)

# gtest_discover_tests(test_kv_on_wal)

# ==============================================
