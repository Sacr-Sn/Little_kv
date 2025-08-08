add_executable(main_kv ./main.cpp)

# 可选择链接内存或文件实现
# target_link_libraries(main_kv PRIVATE kv_on_memory)
# target_link_libraries(main_kv PRIVATE cli kv_levels_sst)
target_link_libraries(main_kv PRIVATE cli kv_alpha)
