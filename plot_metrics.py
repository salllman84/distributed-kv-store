import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load the data
df = pd.read_csv('baseline_latency.csv')

# Calculate relative time in seconds (starting from 0)
df['time_relative'] = df['timestamp'] - df['timestamp'].min()

# Calculate Academic Metrics
avg_latency = df['latency_ms'].mean()
p95_latency = np.percentile(df['latency_ms'], 95)
p99_latency = np.percentile(df['latency_ms'], 99)
max_latency = df['latency_ms'].max()

print(f"--- ACADEMIC BENCHMARK RESULTS ---")
print(f"Total Requests: {len(df)}")
print(f"Average Latency: {avg_latency:.2f} ms")
print(f"95th Percentile: {p95_latency:.2f} ms")
print(f"99th Percentile (Tail): {p99_latency:.2f} ms")
print(f"Max Latency (Worst Case): {max_latency:.2f} ms")
print(f"----------------------------------")

# Generate the Paper-Ready Graph
plt.figure(figsize=(10, 5))
plt.scatter(df['time_relative'], df['latency_ms'], alpha=0.5, color='blue', s=10)
plt.axhline(y=p99_latency, color='red', linestyle='--', label=f'99th Percentile ({p99_latency:.2f} ms)')

plt.title('LSM-Tree Foreground Latency under Asynchronous Compaction')
plt.xlabel('Time (seconds)')
plt.ylabel('Latency (ms)')
plt.legend()
plt.grid(True, linestyle=':', alpha=0.7)

# Save the graph as an image for the paper
plt.savefig('compaction_latency_graph.png', dpi=300)
print("Graph saved as compaction_latency_graph.png")