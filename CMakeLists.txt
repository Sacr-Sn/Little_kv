cmake_minimum_required(VERSION 3.10)
project(LittleKV)

enable_testing()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(src)
add_subdirectory(app)
add_subdirectory(test)
