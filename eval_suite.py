#!/usr/bin/env python3
"""
eval_suite.py — Empirical validation for the Raft/LSM-tree Key-Value store paper.

Modes:
    python3 eval_suite.py ablation    # The 83% p99 latency claim (single-node)
    python3 eval_suite.py threshold   # Justifying the "+2" tripwire heuristic (3-node)
    python3 eval_suite.py chaos       # Data integrity under SIGKILL mid-compaction (3-node)
    python3 eval_suite.py all         # Run every mode sequentially

Prerequisites (must be implemented in C++ before running):
    --striping=true|false          toggle lock striping
    --async_io=true|false          toggle async I/O path
    --tripwire_offset=N            SSTable count above COMPACTION_THRESHOLD that fires the tripwire
    --cluster_size=N               trim the hardcoded 3-node topology down to N
    READ_LOCAL <key>               bypass the leader check and read local state (for follower verification)

Environment overrides:
    KV_BINARY=/path/to/kv_node     default: ./build/kv_node
"""
import csv
import os
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HOST = "127.0.0.1"
BASE_PORT = 8081
NODE_IDS = [1, 2, 3]
PORTS = {nid: BASE_PORT + (nid - 1) for nid in NODE_IDS}
SCRIPT_DIR = Path(__file__).resolve().parent

# Resolve BINARY to an ABSOLUTE path.
# Each Node runs with cwd=<tempdir>, so a relative './build/kv_node' would be
# looked up inside the tempdir and fail with FileNotFoundError. Anchoring to
# SCRIPT_DIR (the directory containing eval_suite.py) fixes this.
_env_binary = os.environ.get("KV_BINARY")
if _env_binary is None:
    BINARY = str((SCRIPT_DIR / "build" / "kv_node").resolve())
else:
    BINARY = str(Path(_env_binary).expanduser().resolve())

ABLATION_N       = 10_000
ABLATION_WORKERS = 8
THRESHOLD_N      = 5_000
THRESHOLD_WORKERS = 8
THRESHOLD_OFFSETS = [2, 4, 8, 16]
CHAOS_N          = 5_000
CHAOS_KILL_AFTER_S = 3.0
CHAOS_CATCHUP_S  = 20.0


# ---------------------------------------------------------------------------
# Low-level RPC helper
# ---------------------------------------------------------------------------
def send_cmd(port: int, cmd: str, timeout: float = 5.0) -> str:
    """
    Open a TCP connection, send a single request line, read a single response.

    The server's handler does exactly one recv() + one send(), so this is
    adequate for localhost testing. Returns '<ERROR: ...>' on failure so
    callers can distinguish transport errors from application responses.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((HOST, port))
        if not cmd.endswith("\n"):
            cmd += "\n"
        s.sendall(cmd.encode())
        data = s.recv(65536)
        return data.decode(errors="replace").strip()
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        return f"<ERROR: {type(e).__name__}>"
    finally:
        s.close()


def wait_port(port: int, timeout: float = 10.0) -> bool:
    """Poll until a TCP connect succeeds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((HOST, port), timeout=0.2)
            s.close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


# ---------------------------------------------------------------------------
# Node wrapper around subprocess.Popen
# ---------------------------------------------------------------------------
class Node:
    def __init__(self, node_id: int, port: int,
                 extra_args: Optional[List[str]] = None,
                 cluster_size: int = 3,
                 workdir: Optional[str] = None):
        self.node_id = node_id
        self.port = port
        self.extra_args = list(extra_args or [])
        self.cluster_size = cluster_size
        # Each node gets its own sandboxed CWD so meta.dat / .snap / SSTables
        # don't leak between runs. tempfile.mkdtemp is unique per call.
        self.workdir = workdir or tempfile.mkdtemp(prefix=f"kv_n{node_id}_")
        self.proc: Optional[subprocess.Popen] = None
        self.stdout_lines: List[str] = []
        self._reader_thread: Optional[threading.Thread] = None

    # ---- process lifecycle -------------------------------------------------
    def _pump_stdout(self, pipe) -> None:
        for raw in iter(pipe.readline, b""):
            self.stdout_lines.append(raw.decode(errors="replace").rstrip())
        pipe.close()

    def start(self) -> None:
        args = [BINARY, str(self.node_id), str(self.port)] + self.extra_args
        if self.cluster_size != 3:
            args.append(f"--cluster_size={self.cluster_size}")
        self.proc = subprocess.Popen(
            args, cwd=self.workdir,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            bufsize=0,
        )
        self._reader_thread = threading.Thread(
            target=self._pump_stdout, args=(self.proc.stdout,), daemon=True)
        self._reader_thread.start()

    def kill(self, sig: int = signal.SIGKILL) -> None:
        if self.proc is None or self.proc.poll() is not None:
            return
        try:
            os.kill(self.proc.pid, sig)
        except ProcessLookupError:
            return
        try:
            self.proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=2)

    def cleanup(self) -> None:
        self.kill()
        shutil.rmtree(self.workdir, ignore_errors=True)

    # ---- protocol helpers --------------------------------------------------
    def rpc(self, cmd: str, timeout: float = 5.0) -> str:
        return send_cmd(self.port, cmd, timeout=timeout)

    def is_leader(self) -> bool:
        # A GET of a non-existent key on a leader returns '(nil)';
        # a follower returns '-MOVED ...' or '-ERROR ...'.
        resp = self.rpc("GET __leader_probe__", timeout=2.0)
        return not (resp.startswith("-MOVED") or resp.startswith("-ERROR")
                    or resp.startswith("<ERROR"))

    def wait_leader(self, timeout: float = 10.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.is_leader():
                return True
            time.sleep(0.1)
        return False

    # ---- log scanning ------------------------------------------------------
    def count_log(self, needle: str) -> int:
        return sum(1 for l in self.stdout_lines if needle in l)


# ---------------------------------------------------------------------------
# Cluster helpers
# ---------------------------------------------------------------------------
def start_cluster(extra_args: Optional[List[str]] = None,
                  cluster_size: int = 3) -> List[Node]:
    nodes = []
    for nid in NODE_IDS[:cluster_size]:
        n = Node(nid, PORTS[nid], extra_args=extra_args, cluster_size=cluster_size)
        n.start()
        nodes.append(n)
    for n in nodes:
        if not wait_port(n.port, timeout=10.0):
            raise RuntimeError(f"node {n.node_id} failed to bind port {n.port}")
    return nodes


def wait_for_cluster_leader(nodes: List[Node], timeout: float = 15.0) -> Optional[Node]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        for n in nodes:
            if n.proc and n.proc.poll() is None and n.is_leader():
                return n
        time.sleep(0.2)
    return None


def count_leader_transfers(nodes: List[Node]) -> int:
    # "Won election" appears once per successful election across the cluster.
    # The FIRST election is not a transfer; subtract 1 and clamp at 0.
    total = sum(n.count_log("Won election! Promoted to LEADER") for n in nodes)
    return max(0, total - 1)


def count_tripwire_fires(nodes: List[Node]) -> int:
    return sum(n.count_log("ASYNC EVENT TRIPWIRE") for n in nodes)


# ---------------------------------------------------------------------------
# Workload
# ---------------------------------------------------------------------------
def blast_writes(port: int, n: int, workers: int = 8) -> List[float]:
    """Fire n SET requests from `workers` threads, return per-RPC latencies (microseconds)."""
    latencies: List[float] = []
    lock = threading.Lock()

    def worker(lo: int, hi: int) -> None:
        local: List[float] = []
        for i in range(lo, hi):
            key = f"k{i:06d}"
            val = f"v{i}"
            t0 = time.perf_counter()
            resp = send_cmd(port, f"SET {key} {val}", timeout=10.0)
            t1 = time.perf_counter()
            # Only count successful SETs in the latency distribution.
            if resp.startswith("OK"):
                local.append((t1 - t0) * 1e6)
        with lock:
            latencies.extend(local)

    chunk = max(1, n // workers)
    threads = []
    for w in range(workers):
        lo = w * chunk
        hi = lo + chunk if w < workers - 1 else n
        t = threading.Thread(target=worker, args=(lo, hi))
        threads.append(t); t.start()
    for t in threads:
        t.join()
    return latencies


def percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    k = max(0, min(len(sorted_vals) - 1, int(round(p * (len(sorted_vals) - 1)))))
    return sorted_vals[k]


def summarize(latencies_us: List[float]) -> Dict[str, float]:
    s = sorted(latencies_us)
    return {
        "n":       len(s),
        "mean_us": statistics.mean(s) if s else float("nan"),
        "p50_us":  percentile(s, 0.50),
        "p99_us":  percentile(s, 0.99),
        "p999_us": percentile(s, 0.999),
    }


def write_csv(path: Path, rows: List[Dict]) -> None:
    if not rows:
        print(f"  (no rows to write to {path.name})")
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"  -> wrote {path}")


# ===========================================================================
# MODE 1 — Ablation Study
# ===========================================================================
def run_ablation() -> None:
    print("\n================ ABLATION STUDY (p99 latency) ================")
    configs = [
        ("single_mutex_sync_io",  ["--striping=false", "--async_io=false", "--enable-tripwire=false"]),
        ("striped_sync_io",       ["--striping=true",  "--async_io=false", "--enable-tripwire=false"]),
        ("single_mutex_async_io", ["--striping=false", "--async_io=true",  "--enable-tripwire=false"]),
        ("striped_async_io",      ["--striping=true",  "--async_io=true",  "--enable-tripwire=false"]),
    ]
    rows = []
    for name, flags in configs:
        print(f"\n--- config: {name}  flags: {flags}")
        node = Node(node_id=1, port=BASE_PORT, extra_args=flags, cluster_size=1)
        try:
            node.start()
            if not wait_port(node.port, timeout=10.0):
                print("  [FAIL] node never bound"); continue
            if not node.wait_leader(timeout=10.0):
                print("  [FAIL] single-node cluster never elected itself"); continue

            lat = blast_writes(node.port, ABLATION_N, ABLATION_WORKERS)
            stats = summarize(lat)
            stats["config"] = name
            rows.append({
                "config":  name,
                "n":       stats["n"],
                "mean_us": round(stats["mean_us"], 2),
                "p50_us":  round(stats["p50_us"], 2),
                "p99_us":  round(stats["p99_us"], 2),
                "p999_us": round(stats["p999_us"], 2),
            })
            print(f"  n={stats['n']}  mean={stats['mean_us']:.1f}us  "
                  f"p50={stats['p50_us']:.1f}us  p99={stats['p99_us']:.1f}us  "
                  f"p99.9={stats['p999_us']:.1f}us")
        finally:
            node.cleanup()

    # Compute the headline "p99 improvement" claim between the two extremes.
    if len(rows) == 4:
        slow = next(r for r in rows if r["config"] == "single_mutex_sync_io")
        fast = next(r for r in rows if r["config"] == "striped_async_io")
        if slow["p99_us"] > 0:
            gain = 100.0 * (slow["p99_us"] - fast["p99_us"]) / slow["p99_us"]
            print(f"\n  HEADLINE: p99 reduction single_mutex+sync -> striped+async = "
                  f"{gain:.1f}%")

    write_csv(SCRIPT_DIR / "ablation_results.csv", rows)


# ===========================================================================
# MODE 2 — Threshold Sensitivity
# ===========================================================================
def run_threshold() -> None:
    print("\n================ THRESHOLD SENSITIVITY (tripwire_offset) ================")
    rows = []
    for offset in THRESHOLD_OFFSETS:
        print(f"\n--- tripwire_offset = {offset}")
        nodes = start_cluster(extra_args=[f"--tripwire_offset={offset}"])
        try:
            leader = wait_for_cluster_leader(nodes, timeout=15.0)
            if leader is None:
                print("  [FAIL] no leader elected"); continue

            t0 = time.perf_counter()
            lat = blast_writes(leader.port, THRESHOLD_N, THRESHOLD_WORKERS)
            elapsed = time.perf_counter() - t0

            stats = summarize(lat)
            throughput = len(lat) / elapsed if elapsed > 0 else float("nan")
            transfers = count_leader_transfers(nodes)
            fires     = count_tripwire_fires(nodes)

            rows.append({
                "tripwire_offset": offset,
                "throughput_wps":  round(throughput, 1),
                "p99_us":          round(stats["p99_us"], 2),
                "mean_us":         round(stats["mean_us"], 2),
                "leader_transfers": transfers,
                "tripwire_fires":   fires,
                "duration_s":       round(elapsed, 3),
            })
            print(f"  throughput={throughput:,.0f} wps  p99={stats['p99_us']:.1f}us  "
                  f"leader_transfers={transfers}  tripwire_fires={fires}")
        finally:
            for n in nodes:
                n.cleanup()

    write_csv(SCRIPT_DIR / "threshold_sensitivity.csv", rows)


# ===========================================================================
# MODE 3 — Chaos / Data Integrity
# ===========================================================================
def background_writer(initial_port: int, n: int,
                      written: List[int], lock: threading.Lock,
                      stop_flag: threading.Event,
                      all_ports: List[int]) -> None:
    """
    Write SET key_{i:05d} value_{i} for i in [0, n), tolerating leader failures.

    On transport error (dead leader), probe every known port until one
    responds as a leader. On '-MOVED <port>', follow the redirect.
    """
    port = initial_port
    i = 0
    while i < n and not stop_flag.is_set():
        key = f"key_{i:05d}"
        val = f"value_{i}"
        success = False

        for attempt in range(400):
            if stop_flag.is_set():
                return

            resp = send_cmd(port, f"SET {key} {val}", timeout=2.0)

            if resp.startswith("OK"):
                with lock:
                    written.append(i)
                success = True
                break

            elif resp.startswith("-MOVED"):
                m = re.search(r"-MOVED\s+(\d+)", resp)
                if m:
                    port = int(m.group(1))
                time.sleep(0.05)

            elif resp.startswith("<ERROR"):
                # Old leader is dead or unreachable. Probe every known port
                # for a live node and adopt whichever answers as leader.
                for candidate in all_ports:
                    probe = send_cmd(candidate, "GET __leader_probe__", timeout=0.5)
                    if probe.startswith("<ERROR"):
                        continue
                    if not probe.startswith("-MOVED") and not probe.startswith("-ERROR"):
                        port = candidate
                        break
                time.sleep(0.15)

            elif resp.startswith("-ERROR"):
                # Election in progress; back off and retry.
                time.sleep(0.15)

            else:
                time.sleep(0.05)

        if not success:
            # Exhausted all retries on this key. Give up on the run.
            break

        i += 1


def verify_all_nodes(nodes: List[Node], n: int) -> bool:
    """Query every node for every key via READ_LOCAL; assert consistency."""
    print(f"\n  Verifying {n} keys on {len(nodes)} nodes via READ_LOCAL...")
    all_pass = True
    for node in nodes:
        mismatches = 0
        missing = 0
        for i in range(n):
            key = f"key_{i:05d}"
            expected = f"value_{i}"
            resp = node.rpc(f"READ_LOCAL {key}", timeout=5.0)
            if resp == "(nil)":
                missing += 1
                if missing <= 3:
                    print(f"    [node {node.node_id}] MISSING {key}")
            elif resp != expected:
                mismatches += 1
                if mismatches <= 3:
                    print(f"    [node {node.node_id}] MISMATCH {key}: "
                          f"got {resp!r} expected {expected!r}")
        if mismatches == 0 and missing == 0:
            print(f"  PASS: node {node.node_id} holds all {n} keys identically.")
        else:
            print(f"  FAIL: node {node.node_id} -> missing={missing}, mismatches={mismatches}")
            all_pass = False
    return all_pass


def run_chaos() -> bool:
    print("\n================ CHAOS / DATA-INTEGRITY ================")
    nodes = start_cluster()
    try:
        leader = wait_for_cluster_leader(nodes, timeout=15.0)
        if leader is None:
            print("  [FAIL] no initial leader"); return False
        print(f"  Initial leader: node {leader.node_id} (port {leader.port})")

        written: List[int] = []
        lock = threading.Lock()
        stop_flag = threading.Event()

        writer = threading.Thread(
            target=background_writer,
            args=(leader.port, CHAOS_N, written, lock, stop_flag,
                  list(PORTS.values())),
            daemon=True,
        )
        writer.start()

        # Let the leader be actively serving (and flushing SSTables) before we kill it.
        time.sleep(CHAOS_KILL_AFTER_S)
        with lock:
            progress = len(written)
        print(f"  Progress at kill time: {progress}/{CHAOS_N} writes acked")

        # ---- SIGKILL the leader mid-compaction -------------------------------
        print(f"  SIGKILL -> node {leader.node_id}")
        leader.kill(signal.SIGKILL)

        # ---- Wait for a surviving follower to become the new leader ----------
        survivors = [n for n in nodes if n is not leader]
        new_leader = wait_for_cluster_leader(survivors, timeout=20.0)
        if new_leader is None:
            print("  [FAIL] cluster failed to elect a new leader"); return False
        print(f"  New leader: node {new_leader.node_id}")

        # Let the writer finish the remaining writes against the new leader.
        writer.join(timeout=180.0)
        if writer.is_alive():
            print("  [WARN] writer still running after 180s; signaling stop")
            stop_flag.set()
            writer.join(timeout=5.0)
        with lock:
            total_acked = len(written)
        print(f"  Total successful writes: {total_acked}/{CHAOS_N}")

        # ---- Restart the dead node in the same workdir -----------------------
        print(f"  Restarting node {leader.node_id} from its own workdir...")
        leader.start()
        if not wait_port(leader.port, timeout=10.0):
            print("  [FAIL] dead node never came back up"); return False

        # Give the cluster time to replay log / install snapshot.
        print(f"  Waiting {CHAOS_CATCHUP_S}s for recovery + catch-up...")
        time.sleep(CHAOS_CATCHUP_S)

        # ---- Verify every node holds every key -------------------------------
        ok = verify_all_nodes(nodes, CHAOS_N)
        if ok and total_acked == CHAOS_N:
            print("\n  PASS: 0 entries lost or reordered.")
        elif ok:
            print(f"\n  PASS (with retries): all nodes consistent; "
                  f"acked={total_acked}/{CHAOS_N}")
        else:
            print("\n  FAIL: divergent state across nodes.")
        return ok
    finally:
        for n in nodes:
           n.cleanup()


# ===========================================================================
# Dispatcher
# ===========================================================================
def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in {"ablation", "threshold", "chaos", "all"}:
        print(__doc__)
        return 2
    mode = sys.argv[1]

    if not Path(BINARY).exists():
        print(f"[FATAL] binary not found: {BINARY}")
        print("        build with cmake, or set KV_BINARY=/path/to/kv_node")
        return 1

    try:
        if mode in ("ablation", "all"):
            run_ablation()
        if mode in ("threshold", "all"):
            run_threshold()
        if mode in ("chaos", "all"):
            run_chaos()
    except KeyboardInterrupt:
        print("\n[INTERRUPTED]")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())