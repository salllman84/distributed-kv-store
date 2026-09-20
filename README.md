# Compaction-Aware Consensus: Distributed Key-Value Store

This repository contains the C++17 implementation of a distributed key-value store built on a custom Raft consensus state machine and a lock-striped Log-Structured Merge (LSM) tree. 

The primary research focus of this system is preventing **Gray Failures** (specifically the "RPC Death Spiral") in distributed storage clusters. By tightly coupling the storage engine's I/O health with the consensus layer, the system proactively demotes Leaders experiencing SSD saturation before network timeouts trigger catastrophic cluster-wide election storms.

## Core Architecture

* **Consensus Layer (Raft):** Custom C++ implementation handling leader election, log replication, and heartbeat dissemination.
* **Storage Engine (LSM-Tree):** 
  * 16-way lock-striped MemTable to eliminate memory ingestion contention.
  * Atomic asynchronous I/O offloading for zero-block SSTable flushing.
* **Compaction-Aware Tripwire:** A lock-free heuristic that monitors pending SSTable flushes. If disk saturation breaches a critical threshold, it bypasses the blocked network loop and triggers a graceful Raft Leader step-down.

## Build Instructions

The project uses CMake and requires GCC/G++ with C++17 support.

```bash
mkdir -p build && cd build
cmake ..
make -j4