cmake_minimum_required(VERSION 3.10)
project(DistributedKVStore VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# ---------------------------------------------------------
# Test-mode toggle
# ---------------------------------------------------------
# Enables test-only code paths (currently: the READ_LOCAL RPC in main.cpp).
#
# Usage:
#   Production:  cmake -S . -B build -DENABLE_TEST_MODE=OFF
#   Test suite:  cmake -S . -B build -DENABLE_TEST_MODE=ON
#
# When ON, -DKV_TEST_MODE is passed to the compiler for all targets in this
# directory and below. When OFF, the READ_LOCAL branch is compiled out.
# ---------------------------------------------------------
option(ENABLE_TEST_MODE "Compile test-only RPCs (e.g. READ_LOCAL) into the binary" OFF)

if(ENABLE_TEST_MODE)
    message(STATUS "KV_TEST_MODE: ENABLED (test-only RPCs will be compiled in)")
    add_compile_definitions(KV_TEST_MODE)
else()
    message(STATUS "KV_TEST_MODE: disabled (production build)")
endif()

# Add the include directory so compiler can find our .hpp files
include_directories(include)

# Find the POSIX thread library
find_package(Threads REQUIRED)

# ---------------------------------------------------------
# Target 1: The Main Server
# ---------------------------------------------------------
add_executable(kv_node
    src/main.cpp
    src/network/socket.cpp
    src/network/server.cpp
    src/common/thread_pool.cpp
    src/raft/log.cpp
    src/raft/consensus.cpp
)
target_link_libraries(kv_node PRIVATE Threads::Threads)

# ---------------------------------------------------------
# Target 2: Storage Engine Unit Test
# ---------------------------------------------------------
add_executable(test_store tests/test_store.cpp)
target_link_libraries(test_store PRIVATE Threads::Threads)

# ---------------------------------------------------------
# Target 3: Network Wrapper Unit Test
# ---------------------------------------------------------
add_executable(test_network
    tests/test_network.cpp
    src/network/socket.cpp
)
target_link_libraries(test_network PRIVATE Threads::Threads)

# ---------------------------------------------------------
# Target 4: YCSB Benchmark (Custom LSM vs LevelDB)
# ---------------------------------------------------------
add_executable(ycsb_benchmark tests/ycsb_benchmark.cpp)
target_link_libraries(ycsb_benchmark PRIVATE leveldb Threads::Threads)