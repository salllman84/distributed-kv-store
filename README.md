# Distributed Key-Value Store

A high-performance, fault-tolerant distributed key-value database written in modern C++. This project implements a complete distributed systems architecture from scratch, capable of surviving network partitions, leader crashes, and massive concurrent load.

## Core Architecture

* **Consensus Layer (Raft):** Implements the Raft consensus algorithm with Leader Election, Log Replication, and Write-Ahead Logging (WAL). 
* **Linearizable Reads:** Prevents "split-brain" stale data during network partitions using Leader Leases.
* **Storage Engine (LSM-Tree):** Replaces basic in-memory maps with a Log-Structured Merge Tree. Absorbs high-velocity writes into a MemTable and sequentially flushes them to immutable SSTables on disk, complete with Tombstone deletion and background Major Compaction.
* **Networking (`epoll`):** Solves the C10K problem using a custom Linux `epoll` event loop (Reactor pattern) to handle massive concurrency asynchronously without exhausting thread pools.

## Building the Project

Requires a Linux environment (or WSL2) with CMake and GCC/Clang.

```bash
mkdir build && cd build
cmake ..
make -j4
```

## Running the Cluster

Launch the 3-node local cluster:
```bash
./scripts/cluster_start.sh
```

Find the current Leader, then connect via Netcat:
```bash
# Check the logs to see which node won the election
cat logs/*.log | grep "Won election"

# Replace 8081 with the actual Leader's port
echo "SET my_key hello_world" | nc 127.0.0.1 8081
echo "GET my_key" | nc 127.0.0.1 8081
echo "DEL my_key" | nc 127.0.0.1 8081
```

## Chaos Engineering Validation

This database includes a custom Chaos Monkey test suite that blasts the cluster with concurrent writes while randomly killing and rebooting nodes to simulate power failures.

```bash
python3 chaos_test.py
```
*The test mathematically verifies that 100% of acknowledged writes survive total cluster chaos.*