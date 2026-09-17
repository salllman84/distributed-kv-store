cmake_minimum_required(VERSION 3.10)
project(DistributedKVStore VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

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