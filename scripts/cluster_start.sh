#!/bin/bash

# Stop script execution immediately if any command fails
set -e

# Navigate to the project root directory regardless of where the script was invoked
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "[Cluster Launcher] Project root: $PROJECT_ROOT"
echo "[Cluster Launcher] Building project..."

mkdir -p build
cd build
cmake ..
make kv_node
cd "$PROJECT_ROOT"

# Create log directory
mkdir -p logs

echo "[Cluster Launcher] Cleaning up previous kv_node instances..."
pkill -f kv_node || true

echo "[Cluster Launcher] Starting 3-node local Raft cluster..."

# Start Node 1 (ID: 1, Port: 8081)
./build/kv_node 1 8081 > logs/node1.log 2>&1 &
NODE1_PID=$!
echo "[Cluster Launcher] Node 1 started (PID: $NODE1_PID) on port 8081"

# Start Node 2 (ID: 2, Port: 8082)
./build/kv_node 2 8082 > logs/node2.log 2>&1 &
NODE2_PID=$!
echo "[Cluster Launcher] Node 2 started (PID: $NODE2_PID) on port 8082"

# Start Node 3 (ID: 3, Port: 8083)
./build/kv_node 3 8083 > logs/node3.log 2>&1 &
NODE3_PID=$!
echo "[Cluster Launcher] Node 3 started (PID: $NODE3_PID) on port 8083"

echo ""
echo "--------------------------------------------------------"
echo "Cluster is running in the background."
echo "View logs:  tail -f logs/node1.log"
echo "Stop cluster: pkill -f kv_node"
echo "--------------------------------------------------------"

# Keep script open to trap Ctrl+C and clean up background processes
trap "echo 'Stopping cluster...'; pkill -f kv_node; exit 0" INT
while true; do
    sleep 1
done