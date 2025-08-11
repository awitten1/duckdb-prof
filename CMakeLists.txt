
cmake_minimum_required(VERSION 3.26)

project(duckdb-bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

set(OVERRIDE_GIT_DESCRIBE v1.3.2)

set(OVERRIDE_NEW_DELETE ON)
set(NATIVE_ARCH ON)
include_directories(${CMAKE_CURRENT_LIST_DIR}/perfcxx)

add_subdirectory(perfcxx)

# set(CMAKE_CXX_FLAGS_RELEASE "-fno-omit-frame-pointer")

find_package(PkgConfig REQUIRED)
set(DISABLE_BUILTIN_EXTENSIONS TRUE)
add_subdirectory(duckdb)

find_package(fmt REQUIRED)

add_executable(main main.cpp)

list(APPEND CMAKE_PREFIX_PATH ${CMAKE_CURRENT_LIST_DIR}/deps/source/jemalloc-5.3.0/jemalloc-5.3.0)
set(ENV{PKG_CONFIG_PATH} "${CMAKE_CURRENT_LIST_DIR}/deps/source/jemalloc-5.3.0/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")

message(STATUS "asdf ${CMAKE_PREFIX_PATH}")
pkg_check_modules(jemalloc REQUIRED IMPORTED_TARGET GLOBAL jemalloc)


# Now bracket portion of duckdb code that is of interest.
target_link_libraries(main PRIVATE perfcxx)
target_link_libraries(main PRIVATE duckdb_static fmt::fmt)
target_link_libraries(main PUBLIC PkgConfig::jemalloc)

