# Compaction-Aware Consensus

A C++20 research prototype exploring **storage-triggered leader step-down in a Raft-based distributed key-value store**.

The project combines a custom Raft consensus implementation with a sharded Log-Structured Merge (LSM) storage engine. The repository contains the implementation, tests, experimental harnesses, raw logs, generated figures, and the accompanying research paper.

> **Research prototype:** This project is intended for experimentation and reproducibility, not production deployment.

## Research Question

Can an LSM-tree storage engine detect local storage degradation early enough to notify the Raft layer and step down the current leader before followers' election timers expire?

The prototype uses SSTable flush latency as a storage-health signal. When the signal crosses a configured threshold, the storage layer sets an atomic flag. The Raft tick thread consumes the flag and, if the node is currently leader, transitions it to `FOLLOWER`.

The mechanism performs **leader step-down**, not target-directed Raft leadership transfer.

## Repository Layout

```text
.
├── include/
│   ├── config.hpp
│   ├── store.hpp
│   ├── raft/
│   │   ├── consensus.hpp
│   │   ├── log.hpp
│   │   └── state.hpp
│   ├── network/
│   └── common/
├── src/
├── tests/
├── eval_suite.py
├── exp_sweep.py
├── plot_paper_figures.py
├── figures/
├── exp_*.log
├── ablation_results.csv
├── threshold_sensitivity.csv
├── paper.tex
├── paper.pdf
├── CMakeLists.txt
└── README.md
```

## Architecture

### Storage Engine

The storage layer is a sharded LSM-tree with:

- **16-way lock-striped MemTable**
  - Each shard has its own `std::shared_mutex`.
  - Writes hash keys to individual shards.

- **Persistent asynchronous flush worker**
  - Frozen MemTables are placed into a bounded queue.
  - A background worker writes them to SSTables.

- **Duplicate-flush protection**
  - An atomic flag prevents multiple writers from enqueueing duplicate flushes.

- **SSTable compaction**
  - Triggered when the number of live SSTable files exceeds a configured threshold.

### Raft Consensus

The Raft layer provides:

- Leader election
- Log replication
- Heartbeats
- Persistent WAL
- Group commit and background `fsync`
- Commit index and term persistence
- Snapshot installation
- Log compaction on restart

### Storage-Health Tripwire

The storage engine measures SSTable flush latency and maintains an EWMA.

When the moving average exceeds the configured threshold:

```text
Storage Engine
      │
      │ atomic flag
      ▼
Raft Tick Thread
      │
      ▼
Leader → Follower
```

The storage layer never directly modifies Raft state. The Raft tick thread performs the state transition under the Raft mutex.

This design avoids direct cross-thread mutation of Raft state by the storage worker.

## Building

### Requirements

- Linux
- CMake 3.10+
- GCC 11+ or Clang 14+
- C++20
- POSIX threads

### Build

```bash
mkdir -p build
cd build

cmake -DENABLE_TEST_MODE=ON ..
make -j$(nproc)
```

`ENABLE_TEST_MODE` enables the `READ_LOCAL` RPC used by the evaluation harness to inspect follower state.

### Generated binaries

```text
build/kv_node
build/test_store
build/test_network
build/ycsb_benchmark
```

## Running the Cluster

### Single Node

```bash
./build/kv_node 1 8081 --cluster_size=1
```

### Three-Node Local Cluster

```bash
./build/kv_node 1 8081 --cluster_size=3 &
./build/kv_node 2 8082 --cluster_size=3 &
./build/kv_node 3 8083 --cluster_size=3 &
```

The evaluation harness gives each node a separate working directory so that WAL and metadata files do not conflict.

### Basic Client Test

Send a request to the leader:

```bash
printf 'SET mykey myvalue\n' | nc -q1 127.0.0.1 8081
```

Read the value:

```bash
printf 'GET mykey\n' | nc -q1 127.0.0.1 8081
```

A request sent to a follower is redirected:

```bash
printf 'SET otherkey othervalue\n' | nc -q1 127.0.0.1 8082
```

Expected:

```text
-MOVED <leader-port>
```

## Reproducing the Experiments

The main evaluation tools are:

### Full Evaluation Suite

```bash
python3 -u eval_suite.py all
```

Individual modes:

```bash
python3 -u eval_suite.py ablation
python3 -u eval_suite.py threshold
python3 -u eval_suite.py chaos
```

These cover:

- LSM configuration ablation
- Tripwire threshold experiments
- Mid-compaction `SIGKILL` durability testing

### Storage-Degradation Sweep

Tripwire enabled:

```bash
python3 -u exp_sweep.py 100 --duration 10 --tripwire on
```

Tripwire disabled:

```bash
python3 -u exp_sweep.py 100 --duration 10 --tripwire off
```

Recovery experiment:

```bash
python3 -u exp_sweep.py 100 --duration 15 --tripwire on --clear-after-stepdown
```

The sweep harness records:

- Warmup throughput
- Degraded throughput
- Tripwire firing time
- Leader step-down
- Elections
- New leader
- Tripwire lead relative to the election timeout

Raw output is written to per-trial log files.

## Generating Figures and Tables

```bash
python3 plot_paper_figures.py
```

Generated artifacts include:

```text
figures/fig1_tripwire_sweep.pdf
figures/fig2_tripwire_lead.pdf
figures/fig3_ablation_tail.pdf

figures/table1_tripwire.tex
figures/table2_ablation.tex
figures/table3_threshold.tex
```

## Rebuilding the Paper

```bash
pdflatex paper.tex
pdflatex paper.tex
```

Run twice to resolve references and cross-references.

## Configuration

Important runtime flags:

| Flag | Default | Description |
|---|---:|---|
| `--cluster_size=N` | `3` | Number of cluster nodes |
| `--striping=true\|false` | `true` | Enable 16-way MemTable striping |
| `--async_io=true\|false` | `false` | Enable asynchronous flush worker |
| `--enable-tripwire=true\|false` | `true` | Enable storage-health tripwire |
| `--fault_inject_flush_latency_ms=N` | `0` | Inject additional flush latency |
| `--fault_inject_active` | `false` | Enable runtime fault injection |

`--tripwire_offset` is retained for compatibility but is currently unused by the mechanism.

## Runtime Signals

| Signal | Effect |
|---|---|
| `SIGINT` / `SIGTERM` | Graceful shutdown |
| `SIGUSR1` | Enable fault injection |
| `SIGUSR2` | Disable fault injection |

Fault injection is intended for experiments only.

## Experimental Harness

### `eval_suite.py`

Provides three evaluation modes:

**Ablation**

Compares LSM configurations using latency measurements.

**Threshold**

Evaluates tripwire threshold behavior and reports throughput, latency, and leadership changes.

**Chaos**

Kills the current leader with `SIGKILL` during compaction, allows the remaining nodes to elect a new leader, restarts the killed node, and verifies the replicated data.

### `exp_sweep.py`

The primary storage-degradation experiment:

1. Starts a fresh three-node cluster.
2. Waits for leader election.
3. Runs parallel writers during warmup.
4. Activates injected storage latency on the current leader.
5. Measures degraded operation.
6. Records tripwire and Raft events.
7. Reports throughput and timing information.

### `plot_paper_figures.py`

Aggregates experiment logs and generates the figures and LaTeX tables used by the paper.

## Current Limitations

This project is deliberately a research prototype.

### No target-directed leadership transfer

The tripwire causes the current leader to step down. The next leader is selected through the normal Raft election process.

This is different from Raft's target-directed leadership-transfer mechanisms.

### No formal correctness proof

The implementation serializes the storage-triggered transition through the Raft event loop, but the project does not currently provide a formal proof that the modified state machine preserves all Raft safety invariants.

### No recovery guarantee

The current recovery experiment does not demonstrate automatic return to pre-fault throughput after storage degradation clears.

The prototype therefore focuses on **fault avoidance**, not complete fault-recovery behavior.

### Localhost evaluation

The current experiments use three processes on a single Linux host. Multi-machine evaluation has not yet been performed.

### No production-system benchmark

etcd, TiKV, and CockroachDB are discussed as related systems and motivation, but are not currently used as experimental baselines.

### No degraded-node quarantine

A node that steps down because of storage degradation can become eligible for leadership again after its self-exclusion period.

## Project Status

| Component | Status |
|---|---|
| LSM storage engine | Complete |
| Raft consensus | Functional |
| Persistent WAL | Functional |
| Storage-health tripwire | Functional |
| Unit tests | Available |
| Chaos durability test | 4/4 successful runs |
| Paper | Draft |
| Leader-only degradation experiment | Not yet completed |
| Multi-machine evaluation | Not yet completed |
| Formal correctness argument | Not yet completed |

## Citation

If you use this implementation or build upon this work, please cite:

```bibtex
@misc{khan2026stepdown,
  author = {Salman Khan},
  coauthor = {Zahid Hassan},
  title  = {Storage-Triggered Leader Step-Down in a Raft-Based Key-Value Store},
  year   = {2026},
  note   = {Preprint}
}
```

## License

- **Code:** MIT License
- **Paper:** CC-BY-4.0