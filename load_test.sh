#!/bin/bash
echo "Spawning 500 concurrent SET requests..."

# Spawn 500 network connections in the background
for i in {1..500}; do
    echo "SET concurrent_key$i epoll_works" | nc 127.0.0.1 8081 > /dev/null &
done

# Wait for all background processes to finish
wait
echo "All 500 requests completed!"
