
cmake_minimum_required(VERSION 3.26)

project(duckdb-bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

set(OVERRIDE_GIT_DESCRIBE v1.3.2)

set(OVERRIDE_NEW_DELETE ON)
set(NATIVE_ARCH ON)
include_directories(${CMAKE_CURRENT_LIST_DIR}/perfcxx)

set(CMAKE_SKIP_INSTALL_RULES ON)
add_subdirectory(perfcxx)
add_subdirectory(duckdb EXCLUDE_FROM_ALL)

find_package(fmt REQUIRED)

add_executable(main main.cpp)

# Now bracket portion of duckdb code that is of interest.
target_link_libraries(main PRIVATE perfcxx)
target_link_libraries(main PRIVATE duckdb_static fmt::fmt)

