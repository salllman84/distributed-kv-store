# Compaction-Aware Consensus: Distributed Key-Value Store

This repository contains the C++ implementation of a distributed key-value store built on a custom Raft consensus state machine and a lock-striped Log-Structured Merge (LSM) tree.

The primary research focus of this system is preventing **Gray Failures** (specifically the "RPC Death Spiral") in distributed storage clusters. By tightly coupling the storage engine's I/O health with the consensus layer, the system proactively demotes Leaders experiencing SSD saturation before network timeouts trigger catastrophic cluster-wide election storms.

## Core Architecture

* **Consensus Layer (Raft):** Custom C++ implementation handling leader election, log replication, and heartbeat dissemination.
* **Storage Engine (LSM-Tree):**
* 16-way lock-striped MemTable to eliminate memory ingestion contention.
* Atomic asynchronous I/O offloading for zero-block SSTable flushing.


* **Compaction-Aware Tripwire:** A lock-free heuristic that monitors pending SSTable flushes. If disk saturation breaches a critical threshold, it bypasses the blocked network loop and triggers a graceful Raft Leader step-down.

## Build Instructions

The project uses CMake. Ensure you have GCC/G++ (C++17 or higher) installed.

```bash
# Clean existing cache and build
rm -rf build/*
cd build
cmake ..
make -j4

```

## Running a Local Cluster

You can spin up a 3-node cluster locally. By default, the nodes run with the Compaction-Aware Tripwire **enabled**.

```bash
./build/kv_node 1 8081 &
./build/kv_node 2 8082 &
./build/kv_node 3 8083 &

```

## Running the Ablation Study (Gray Failure Benchmark)

To reproduce the Gray Failure and validate the necessity of the tripwire, you can disable the mechanism using runtime feature toggles.

**1. Start the cluster with the tripwire disabled:**

```bash
./build/kv_node 1 8081 --enable-tripwire=false &
./build/kv_node 2 8082 --enable-tripwire=false &
./build/kv_node 3 8083 --enable-tripwire=false &

```

**2. Execute the benchmark:**
Wait for the cluster to elect a leader (e.g., Node 1 on port 8081), then execute the heavy write payload to force an SSD stall.

```bash
python3 authentic_benchmark.py 8081

```

**3. Generate the Telemetry Graph:**
Once the storage engine overloads and the cluster deadlocks (triggering an infinite election loop), kill the nodes and generate the empirical RPC throughput graph.

```bash
pkill -f kv_node
python3 plot_telemetry.py

```

This will read the generated `logs/node_*_telemetry.csv` files and output `gray_failure_rpc_spike.png`, visualizing the RPC Death Spiral.

## Repository Structure

* `src/raft/` - Core consensus logic, RPC handling, and state machine transitions.
* `src/network/` - Asynchronous socket multiplexing (epoll).
* `include/` - Header definitions, including `config.hpp` for feature toggles.
* `tests/` - Unit tests and C++ baseline benchmarks.
* `scripts/` - Python telemetry plotters and workload generators.