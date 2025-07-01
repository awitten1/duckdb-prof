
cmake_minimum_required(VERSION 3.26)

project(duckdb-bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

set(OVERRIDE_GIT_DESCRIBE v1.3.1)
add_subdirectory(duckdb)
include_directories(${CMAKE_CURRENT_LIST_DIR}/perfcxx)

find_package(fmt REQUIRED)

add_executable(main main.cpp)
target_link_libraries(main duckdb_static fmt::fmt)

