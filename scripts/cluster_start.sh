#!/bin/bash
pkill -f kv_node
sleep 1
rm -rf data logs
mkdir -p logs data/node1 data/node2 data/node3

echo "[Cluster Launcher] Starting 3-node local Raft cluster with isolated storage..."
(cd data/node1 && ../../build/kv_node 1 8081 > ../../logs/node1.log 2>&1 &)
(cd data/node2 && ../../build/kv_node 2 8082 > ../../logs/node2.log 2>&1 &)
(cd data/node3 && ../../build/kv_node 3 8083 > ../../logs/node3.log 2>&1 &)

echo "Cluster is running in the background."
