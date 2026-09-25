#!/usr/bin/env python3
"""
exp_sweep.py — Storage-degradation experiment.

Hypothesis: for some range of disk degradation, the flush-latency tripwire
fires before Raft's natural election timeout.

Run:
    python3 exp_sweep.py <latency_ms> [--duration 10] [--warmup 2]
                         [--tripwire on|off] [--clear-after-stepdown]

The cluster starts healthy (no injection). After --warmup seconds of steady
state, SIGUSR1 is sent to the current leader, activating the fault injector.
The harness then records when the tripwire fires, when an election starts,
and when a new leader is elected, all relative to the SIGUSR1 timestamp.

Why 16 writer threads: a single-threaded writer is bounded by 1/latency.
At 100 ms RPC latency, a single thread can push at most 10 wps regardless
of how fast the cluster can actually serve. Parallel writers make the
harness measure the cluster, not the client.
"""
import argparse
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import List, Optional, Tuple

BINARY    = str((Path(__file__).resolve().parent / "build" / "kv_node").resolve())
HOST      = "127.0.0.1"
PORTS     = {1: 8081, 2: 8082, 3: 8083}
NUM_WRITERS = 16


# ---------------------------------------------------------------------------
# Low-level RPC
# ---------------------------------------------------------------------------
def send_cmd(port: int, cmd: str, timeout: float = 2.0) -> str:
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((HOST, port))
        if not cmd.endswith("\n"):
            cmd += "\n"
        s.sendall(cmd.encode())
        return s.recv(65536).decode(errors="replace").strip()
    except Exception as e:
        return f"<ERR:{type(e).__name__}>"
    finally:
        s.close()


def wait_port(port: int, timeout: float = 10.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            s = socket.create_connection((HOST, port), timeout=0.2)
            s.close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


# ---------------------------------------------------------------------------
# Node wrapper
# ---------------------------------------------------------------------------
class Node:
    def __init__(self, nid: int, port: int, extra_args: List[str]):
        self.nid = nid
        self.port = port
        self.extra_args = extra_args
        self.workdir = tempfile.mkdtemp(prefix=f"exp_n{nid}_")
        self.proc: Optional[subprocess.Popen] = None
        self.events: List[Tuple[float, str]] = []
        self._lock = threading.Lock()

    def _reader(self) -> None:
        for line in iter(self.proc.stdout.readline, b""):
            ts = time.monotonic()
            text = line.decode(errors="replace").rstrip()
            with self._lock:
                self.events.append((ts, text))

    def start(self) -> None:
        args = [BINARY, str(self.nid), str(self.port)] + self.extra_args + ["--cluster_size=3"]
        self.proc = subprocess.Popen(
            args, cwd=self.workdir,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
        threading.Thread(target=self._reader, daemon=True).start()

    def kill(self, sig: int = signal.SIGKILL) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                os.kill(self.proc.pid, sig)
            except ProcessLookupError:
                pass
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def cleanup(self) -> None:
        self.kill()
        shutil.rmtree(self.workdir, ignore_errors=True)

    def snapshot(self) -> List[Tuple[float, str]]:
        with self._lock:
            return list(self.events)


# ---------------------------------------------------------------------------
# Cluster helpers
# ---------------------------------------------------------------------------
def start_cluster(latency_ms: int, tripwire: bool) -> List[Node]:
    args = [
        f"--fault_inject_flush_latency_ms={latency_ms}",
        f"--enable-tripwire={'true' if tripwire else 'false'}",
        "--striping=true",
        "--async_io=true",
    ]
    nodes: List[Node] = []
    for nid, port in PORTS.items():
        n = Node(nid, port, args)
        n.start()
        nodes.append(n)
    for n in nodes:
        if not wait_port(n.port, timeout=10.0):
            raise RuntimeError(f"node {n.nid} failed to bind port {n.port}")
    return nodes


def find_leader(nodes: List[Node], timeout: float = 15.0) -> Optional[Node]:
    end = time.time() + timeout
    while time.time() < end:
        for n in nodes:
            if n.proc and n.proc.poll() is None:
                resp = send_cmd(n.port, "GET __probe__", timeout=1.0)
                if resp == "(nil)":
                    return n
        time.sleep(0.2)
    return None


# ---------------------------------------------------------------------------
# Parallel writer workers
# ---------------------------------------------------------------------------
def writer_worker(stop_flag: threading.Event,
                  written: List[int],
                  lock: threading.Lock,
                  all_ports: List[int],
                  worker_id: int) -> None:
    """
    One concurrent writer thread. Generates keys with a worker-specific
    prefix so threads never collide. Follows -MOVED redirects. Re-probes
    for a new leader on transport errors.
    """
    port: Optional[int] = None
    i = 0
    while not stop_flag.is_set() and i < 100000:
        if port is None:
            for p in all_ports:
                probe = send_cmd(p, "GET __probe__", timeout=0.5)
                if probe == "(nil)":
                    port = p
                    break
            if port is None:
                time.sleep(0.05)
                continue

        key = f"k{worker_id:03d}_{i:06d}"
        resp = send_cmd(port, f"SET {key} v{i}", timeout=2.0)

        if resp.startswith("OK"):
            with lock:
                written.append(1)
            i += 1
        elif resp.startswith("-MOVED"):
            m = re.search(r"-MOVED\s+(\d+)", resp)
            if m:
                port = int(m.group(1))
        elif resp.startswith("<ERROR"):
            port = None           # leader is dead; re-probe
            time.sleep(0.02)
        else:
            time.sleep(0.01)


def start_writers(stop_flag: threading.Event,
                  written: List[int],
                  lock: threading.Lock) -> List[threading.Thread]:
    workers = [
        threading.Thread(
            target=writer_worker,
            args=(stop_flag, written, lock, list(PORTS.values()), wid),
            daemon=True,
        )
        for wid in range(NUM_WRITERS)
    ]
    for w in workers:
        w.start()
    return workers


def stop_writers(stop_flag: threading.Event,
                 workers: List[threading.Thread]) -> None:
    stop_flag.set()
    for w in workers:
        w.join(timeout=2.0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("latency_ms", type=int)
    ap.add_argument("--duration", type=float, default=10.0)
    ap.add_argument("--warmup",   type=float, default=2.0)
    ap.add_argument("--tripwire", choices=["on", "off"], default="on")
    ap.add_argument("--clear-after-stepdown", action="store_true")
    args = ap.parse_args()

    tripwire_on = args.tripwire == "on"
    print(f"[exp] latency={args.latency_ms}ms  tripwire={args.tripwire}  "
          f"warmup={args.warmup}s  duration={args.duration}s  "
          f"writers={NUM_WRITERS}")

    nodes = start_cluster(args.latency_ms, tripwire_on)
    try:
        print("[exp] waiting for initial leader ...")
        leader = find_leader(nodes, timeout=15.0)
        if not leader:
            print("[exp] NO LEADER — aborting")
            return
        print(f"[exp] leader = node {leader.nid}  PID={leader.proc.pid}")

        # ---- Start parallel writers -----------------------------------
        stop_flag = threading.Event()
        written: List[int] = []
        lock = threading.Lock()
        workers = start_writers(stop_flag, written, lock)

        # ---- Warmup ---------------------------------------------------
        time.sleep(args.warmup)
        warm = len(written)
        warm_wps = warm / args.warmup
        print(f"[exp] warmup: {warm} writes in {args.warmup:.1f}s = {warm_wps:.0f} wps")

        # ---- Inject fault ---------------------------------------------
        t_inject = time.monotonic()
        print(f"[exp] t_inject  → SIGUSR1 to node {leader.nid}")
        os.kill(leader.proc.pid, signal.SIGUSR1)

        # ---- Optional: clear the fault after tripwire fires -----------
        def clear_after_stepdown() -> None:
            deadline = time.time() + 15.0
            while time.time() < deadline:
                for _, line in leader.snapshot():
                    if "Gracefully stepping down to FOLLOWER" in line:
                        print(f"[exp] step-down observed; SIGUSR2 → node {leader.nid}")
                        try:
                            os.kill(leader.proc.pid, signal.SIGUSR2)
                        except ProcessLookupError:
                            pass
                        return
                time.sleep(0.1)
            print("[exp] WARN: step-down not observed within 15s; skipping clear")

        if args.clear_after_stepdown:
            threading.Thread(target=clear_after_stepdown, daemon=True).start()

        # ---- Degraded window ------------------------------------------
        time.sleep(args.duration)
        total = len(written)
        deg = total - warm
        deg_wps = deg / args.duration
        print(f"[exp] degraded: {deg} writes in {args.duration:.1f}s = {deg_wps:.0f} wps")

        stop_writers(stop_flag, workers)

        # ---- Event timeline -------------------------------------------
        print("\n[exp] === Event timeline (relative to t_inject) ===\n")
        events: List[Tuple[float, int, str]] = []
        for n in nodes:
            for ts, line in n.snapshot():
                events.append((ts, n.nid, line))
        events.sort()

        markers = [
            ("FLUSH-LATENCY TRIPWIRE",                "TRIPWIRE_FIRE"),
            ("ASYNC EVENT TRIPWIRE: STORAGE OVERLOAD","TRIPWIRE_RAFT"),
            ("Gracefully stepping down to FOLLOWER",  "STEP_DOWN"),
            ("Election timeout expired",              "ELECTION_START"),
            ("Won election! Promoted to LEADER",      "NEW_LEADER"),
        ]
        for ts, nid, line in events:
            for pat, tag in markers:
                if pat in line:
                    rel = ts - t_inject
                    print(f"  {rel:+.3f}s  [node {nid}] {tag:<14}  {line[:90]}")
                    break

        # ---- Summary --------------------------------------------------
        print("\n[exp] === Summary ===")
        tripwire_times = [ts - t_inject for ts, _, l in events
                          if "FLUSH-LATENCY TRIPWIRE" in l]
        election_times = [ts - t_inject for ts, _, l in events
                          if "Election timeout expired" in l and ts > t_inject]
        new_leader_times = [ts - t_inject for ts, _, l in events
                            if "Won election! Promoted to LEADER" in l and ts > t_inject]

        print(f"  tripwire_fired       = {len(tripwire_times) > 0}")
        if tripwire_times:
            print(f"  first_tripwire_ms    = {min(tripwire_times)*1000:.0f}")
        print(f"  election_fired       = {len(election_times) > 0}")
        if election_times:
            print(f"  first_election_ms    = {min(election_times)*1000:.0f}")
        print(f"  leadership_changed   = {len(new_leader_times) > 0}")
        if new_leader_times:
            print(f"  new_leader_ms        = {min(new_leader_times)*1000:.0f}")
        if tripwire_times and election_times:
            lead = (min(election_times) - min(tripwire_times)) * 1000
            print(f"  tripwire_lead_ms     = {lead:+.0f}")
        print(f"  warmup_wps           = {warm_wps:.0f}")
        print(f"  degraded_wps         = {deg_wps:.0f}")
        if warm_wps > 0:
            print(f"  throughput_ratio     = {deg_wps / warm_wps:.3f}")

    finally:
        for n in nodes:
            n.cleanup()


if __name__ == "__main__":
    main()