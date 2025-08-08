# 内存KV实现
# file(GLOB_RECURSE MEM_SRC kv_on_memory/impls/*.cpp)
# add_library(kv_on_memory STATIC ${MEM_SRC})
# target_include_directories(kv_on_memory PUBLIC kv_on_memory/headers)

# 文件KV实现
# file(GLOB_RECURSE FILE_SRC kv_on_file/impls/*.cpp)
# add_library(kv_on_file STATIC ${FILE_SRC})
# target_include_directories(kv_on_file PUBLIC kv_on_file/headers)

# wal
# file(GLOB_RECURSE WAL_SRC kv_on_wal/impls/*.cpp)
# add_library(kv_on_wal STATIC ${WAL_SRC})
# target_include_directories(kv_on_wal PUBLIC kv_on_wal/headers)

# SkipList
# file(GLOB_RECURSE SL_SRC kv_on_skiplist/impls/*.cpp)
# add_library(kv_on_skiplist STATIC ${SL_SRC})
# target_include_directories(kv_on_skiplist PUBLIC kv_on_skiplist/headers)


# sst
# file(GLOB_RECURSE SST_SRC kv_on_sst/impls/*.cpp)
# add_library(kv_on_sst STATIC ${SST_SRC})
# target_include_directories(kv_on_sst PUBLIC kv_on_sst/headers)

# levels_sst
# file(GLOB_RECURSE LEVELS_SRC kv_levels_sst/impls/*.cpp)
# add_library(kv_levels_sst STATIC ${LEVELS_SRC})
# target_include_directories(kv_levels_sst PUBLIC kv_levels_sst/headers)

# levels_four L0 -> L1 -> L2 -> L3
# file(GLOB_RECURSE LEVELS_SRC kv_levels_four/impls/*.cpp)
# add_library(kv_levels_four STATIC ${LEVELS_SRC})
# target_include_directories(kv_levels_four PUBLIC kv_levels_four/headers)

# ------------------------ 外部库  -----------------------------
# XXHASH
# find xxHash lib
if(NOT TARGET PkgConfig::XXHASH)
    find_path(XXHASH_INCLUDE_DIR xxhash.h
              PATHS /usr/include /usr/local/include)
    find_library(XXHASH_LIBRARY NAMES xxhash libxxhash
                PATHS /usr/lib /usr/lib/x86_64-linux-gnu /usr/local/lib)
    
    if(XXHASH_INCLUDE_DIR AND XXHASH_LIBRARY)
        message(STATUS "Found xxHash: ${XXHASH_LIBRARY}")
        add_library(xxhash::xxhash UNKNOWN IMPORTED)
        set_target_properties(xxhash::xxhash PROPERTIES
            IMPORTED_LOCATION "${XXHASH_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${XXHASH_INCLUDE_DIR}")
    else()
        message(FATAL_ERROR "xxHash not found! Please install libxxhash-dev")
    endif()
endif()



# -----------------------------------------------------

# # import bloom filter
# file(GLOB_RECURSE LEVELS_SRC kv_levels_bloom/impls/*.cpp)
# add_library(kv_levels_bloom STATIC ${LEVELS_SRC})
# target_include_directories(kv_levels_bloom PUBLIC kv_levels_bloom/headers)



# # cli
# file(GLOB_RECURSE CLI_SRC cli/impls/*.cpp)
# add_library(cli STATIC ${CLI_SRC})
# target_include_directories(cli PUBLIC cli/headers)
# target_link_libraries(cli PRIVATE kv_levels_bloom xxhash::xxhash)

# -----------------------------------------------------

# # import thread safe pre
# file(GLOB_RECURSE LEVELS_SRC kv_thread_safe_pre/impls/*.cpp)
# add_library(kv_thread_safe_pre STATIC ${LEVELS_SRC})
# target_include_directories(kv_thread_safe_pre PUBLIC kv_thread_safe_pre/headers)



# # cli
# file(GLOB_RECURSE CLI_SRC cli/impls/*.cpp)
# add_library(cli STATIC ${CLI_SRC})
# target_include_directories(cli PUBLIC cli/headers)
# target_link_libraries(cli PRIVATE kv_thread_safe_pre xxhash::xxhash)

# -----------------------------------------------------

# # import multiple threads (主线程get/put/del + 后台线程flush/compact)
# file(GLOB_RECURSE LEVELS_SRC kv_threads/impls/*.cpp)
# add_library(kv_threads STATIC ${LEVELS_SRC})
# target_include_directories(kv_threads PUBLIC kv_threads/headers)



# # cli
# file(GLOB_RECURSE CLI_SRC cli/impls/*.cpp)
# add_library(cli STATIC ${CLI_SRC})
# target_include_directories(cli PUBLIC cli/headers)
# target_link_libraries(cli PRIVATE kv_threads xxhash::xxhash)

# -----------------------------------------------------

find_package(Snappy REQUIRED)
find_package(ZLIB REQUIRED)

# # meta version
# file(GLOB_RECURSE LEVELS_SRC kv_meta/impls/*.cpp)
# add_library(kv_meta STATIC ${LEVELS_SRC})
# target_include_directories(kv_meta PUBLIC kv_meta/headers)

# alpha version
file(GLOB_RECURSE LEVELS_SRC kv_alpha/impls/*.cpp)
add_library(kv_alpha STATIC ${LEVELS_SRC})
target_include_directories(kv_alpha PUBLIC kv_alpha/headers)



# cli
file(GLOB_RECURSE CLI_SRC cli/impls/*.cpp)
add_library(cli STATIC ${CLI_SRC})
target_include_directories(cli PUBLIC cli/headers)
target_link_libraries(cli PRIVATE kv_alpha xxhash::xxhash Snappy::snappy ZLIB::ZLIB)
    



