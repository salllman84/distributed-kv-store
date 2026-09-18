import socket
import time
import random
import threading
import subprocess

PORTS = [8081, 8082, 8083]
current_leader = 8081
acknowledged_writes = {}

def send_cmd(cmd, max_retries=10):
    global current_leader
    
    for _ in range(max_retries):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect(('127.0.0.1', current_leader))
            s.sendall((cmd + "\n").encode())
            resp = s.recv(1024).decode().strip()
            s.close()
            
            if resp.startswith("-MOVED"):
                current_leader = int(resp.split()[1])
                time.sleep(0.1) # Give the new leader time to establish lease
                continue
            elif resp.startswith("-ERROR"):
                time.sleep(0.5) # Wait out the election
                continue
                
            return resp
        except Exception:
            # Connection failed, leader is likely dead. Pick a random node to ask for redirect.
            time.sleep(0.5)
            current_leader = random.choice(PORTS)
    return None

def writer_task():
    print("[WRITER] Starting high-velocity data ingestion...")
    for i in range(1, 151):
        key = f"chaos_key_{i}"
        val = f"val_{i}_{random.randint(1000,9999)}"
        
        resp = send_cmd(f"SET {key} {val}")
        
        # CRITICAL: We only guarantee data that was explicitly ACKNOWLEDGED by the cluster
        if resp and resp.startswith("OK"):
            acknowledged_writes[key] = val
            
        time.sleep(0.05) # Throttle slightly to ensure we overlap with elections

def chaos_task():
    time.sleep(1.0) # Let the writer get started
    for i in range(4): # Trigger 4 separate server crashes
        node = random.randint(1, 3)
        print(f"\n[CHAOS] Simulating power failure on Node {node}...")
        # Use -9 to bypass graceful shutdown, forcing the node to rely on its WAL/Snapshots
        subprocess.run(f"pkill -9 -f 'kv_node {node}'", shell=True, stderr=subprocess.DEVNULL)
        
        time.sleep(2.5) # Let the cluster elect a new leader and writer experience failure
        
        print(f"[CHAOS] Restoring power to Node {node}...")
        subprocess.Popen(f"./build/kv_node {node} 808{node} >> logs/node{node}.log 2>&1", shell=True)
        time.sleep(1.5)

if __name__ == "__main__":
    print("=== STARTING DISTRIBUTED CHAOS TEST ===")
    
    t1 = threading.Thread(target=writer_task)
    t2 = threading.Thread(target=chaos_task)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    print("\n[SYSTEM] Chaos phase complete. Allowing 3 seconds for cluster replication to stabilize...")
    time.sleep(3)
    
    expected_count = len(acknowledged_writes)
    print(f"\n=== VERIFICATION PHASE ===")
    print(f"Target: Verify {expected_count} strongly-consistent writes survived the crashes.")
    
    success = 0
    for key, expected_val in acknowledged_writes.items():
        resp = send_cmd(f"GET {key}")
        if resp == expected_val:
            success += 1
        else:
            print(f" DATA CORRUPTION! Key {key}: Expected '{expected_val}', Got '{resp}'")
            
    print("\n=== FINAL RESULTS ===")
    if success == expected_count and expected_count > 0:
        print(f" PASSED: 100% Data Integrity. All {success} keys mathematically verified.")
    else:
        print(f" FAILED: Survived {success}/{expected_count}. Check Raft logs for lost commits.")
