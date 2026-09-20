#!/bin/bash
export ETCDCTL_API=3

echo "Blasting etcd Leader (Node 1) with writes..."
# Start a background write loop
for i in {1..2000}; do
    etcdctl --endpoints=127.0.0.1:2379 put key_$i "heavy_payload_data" > /dev/null 2>&1 &
done

sleep 1
echo "[FAULT INJECTION] Simulating 5000ms SSD compaction stall on Node 1..."
NODE1_PID=$(pgrep -f "name infra1")

# Freeze the process (simulating a blocked I/O thread)
kill -SIGSTOP $NODE1_PID
sleep 5
# Unfreeze the process
kill -SIGCONT $NODE1_PID

echo "Stall complete. Waiting for background writes to finish..."
wait
echo "Benchmark finished. Checking etcd logs for Gray Failure symptoms..."
grep -i "took too long" node1_etcd.log
grep -i "dropped" node*_etcd.log
grep -i "leader changed" node*_etcd.log