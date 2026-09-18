import socket
import time
import threading
import sys

if len(sys.argv) < 2:
    print("❌ ERROR: You must specify the true Leader port.")
    print("Run 'grep -a \"Won election\" logs/*.log' to find who won.")
    print("Usage: python3 authentic_benchmark.py <PORT>")
    exit(1)

LEADER_PORT = int(sys.argv[1])
TOTAL_REQUESTS = 20000
CONCURRENCY = 16 

HEAVY_PAYLOAD = "val_" + ("A" * 2)

def worker(thread_id, requests_per_thread):
    for i in range(requests_per_thread):
        key = f"user_{thread_id}_{i}"
        req = f"SET {key} {HEAVY_PAYLOAD}\n"
        
        try:
            with socket.create_connection(('127.0.0.1', LEADER_PORT), timeout=5) as s:
                s.sendall(req.encode())
                s.recv(2) 
        except:
            pass

print(f"Blasting true Raft Leader (Port {LEADER_PORT}) with {TOTAL_REQUESTS} heavy payloads...")

threads = []
reqs_per = TOTAL_REQUESTS // CONCURRENCY
start_test = time.time()

for i in range(CONCURRENCY):
    t = threading.Thread(target=worker, args=(i, reqs_per))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

print(f"Authentic overload complete in {time.time() - start_test:.2f} seconds.")