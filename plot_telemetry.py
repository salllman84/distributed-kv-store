import csv
import glob
import matplotlib.pyplot as plt
from collections import defaultdict

def plot_telemetry():
    files = glob.glob("logs/node_*_telemetry.csv")
    if not files:
        print("No telemetry CSVs found in logs/")
        return

    plt.figure(figsize=(12, 6))

    for file in files:
        node_id = file.split('_')[1]
        times, rpcs = [], []
        
        with open(file, 'r') as f:
            reader = csv.reader(f)
            next(reader) # Skip header
            for row in reader:
                if len(row) >= 3:
                    # row format: Timestamp_ms, Raft_State, Outgoing_RPCs_Per_Sec
                    times.append(int(row[0]))
                    rpcs.append(float(row[2]))

        if not times:
            continue
            
        # Normalize time to start at 0 seconds
        start_time = min(times)
        normalized_times = [(t - start_time) / 1000.0 for t in times]
        
        plt.plot(normalized_times, rpcs, label=f'Node {node_id} RPCs/sec', linewidth=2)

    plt.title("Raft RPC Throughput During Gray Failure (Tripwire Disabled)", fontsize=14, fontweight='bold')
    plt.xlabel("Time (seconds)", fontsize=12)
    plt.ylabel("Outgoing RPCs / Sec", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.tight_layout()
    
    plt.savefig("gray_failure_rpc_spike.png", dpi=300)
    print("Graph saved as gray_failure_rpc_spike.png")

if __name__ == "__main__":
    plot_telemetry()