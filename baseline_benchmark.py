import socket
import time
import csv
import threading

# Configuration
SERVER_IP = '127.0.0.1'
SERVER_PORT = 8081
TOTAL_REQUESTS = 20000
CONCURRENCY = 10  # Number of parallel threads

latencies = []
lock = threading.Lock()

def worker(thread_id, requests_per_thread):
    for i in range(requests_per_thread):
        key = f"key_{thread_id}_{i}"
        val = f"val_{i}_payload_data"
        req = f"SET {key} {val}\n"
        
        # Measure exact network & processing time
        start_time = time.perf_counter()
        
        try:
            with socket.create_connection((SERVER_IP, SERVER_PORT), timeout=5) as s:
                s.sendall(req.encode())
                s.recv(1024) # Wait for 200 OK
        except Exception as e:
            pass
            
        end_time = time.perf_counter()
        
        latency_ms = (end_time - start_time) * 1000
        
        with lock:
            latencies.append((time.time(), latency_ms))

print(f"Blasting server with {TOTAL_REQUESTS} SET requests...")

threads = []
reqs_per = TOTAL_REQUESTS // CONCURRENCY

start_test = time.time()
for i in range(CONCURRENCY):
    t = threading.Thread(target=worker, args=(i, reqs_per))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

print(f"Test completed in {time.time() - start_test:.2f} seconds.")

# Save to CSV for our research paper graphs
with open('baseline_latency.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "latency_ms"])
    writer.writerows(latencies)
    
print("Saved metrics to baseline_latency.csv")