#!/usr/bin/env python3
"""
plot_paper_figures.py — Generate publication-quality figures and LaTeX tables
from raw experiment logs.

Run:
    python3 plot_paper_figures.py

Reads:
    exp_on_<L>_t<k>.log, exp_off_<L>_t<k>.log
    ablation_results.csv
    threshold_sensitivity.csv

Writes (under figures/):
    fig1_tripwire_sweep.pdf / .png
    fig2_tripwire_lead.pdf  / .png
    fig3_ablation_tail.pdf  / .png
    table1_tripwire.tex
    table2_ablation.tex
    table3_threshold.tex
"""
import csv
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

plt.rcParams.update({
    "font.family": "serif",
    "font.size": 9,
    "axes.labelsize": 10,
    "axes.titlesize": 10,
    "legend.fontsize": 8.5,
    "xtick.labelsize": 8.5,
    "ytick.labelsize": 8.5,
    "axes.grid": True,
    "grid.alpha": 0.35,
    "grid.linestyle": ":",
    "figure.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.02,
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
})

ROOT   = Path(__file__).resolve().parent
FIGDIR = ROOT / "figures"
FIGDIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Log parser
# ---------------------------------------------------------------------------
def parse_exp_log(path: Path) -> Optional[Dict]:
    if not path.exists():
        return None
    text = path.read_text(errors="replace")
    out: Dict[str, float] = {}

    def grab(pattern, key, cast=int):
        m = re.search(pattern, text)
        if m:
            out[key] = cast(m.group(1))

    grab(r"warmup:\s+\d+\s+writes\s+in\s+[\d.]+s\s+=\s+(\d+)\s+wps",
         "warmup_wps")
    grab(r"degraded:\s+\d+\s+writes\s+in\s+[\d.]+s\s+=\s+(\d+)\s+wps",
         "degraded_wps")
    grab(r"throughput_ratio\s+=\s+([\d.]+)", "ratio", float)
    grab(r"first_tripwire_ms\s+=\s+([-+]?\d+)", "first_tripwire_ms")
    grab(r"first_election_ms\s+=\s+([-+]?\d+)", "first_election_ms")

    # The +/− prefix is the fix vs the previous version.
    grab(r"tripwire_lead_ms\s+=\s+([-+]?\d+)", "tripwire_lead_ms")

    m = re.search(r"tripwire_fired\s+=\s+(\w+)", text)
    if m:
        out["tripwire_fired"] = 1 if m.group(1) == "True" else 0

    return out if "ratio" in out else None


def collect_sweep_data() -> Dict[str, Dict[int, List[Dict]]]:
    data: Dict[str, Dict[int, List[Dict]]] = {
        "on":  defaultdict(list),
        "off": defaultdict(list),
    }
    for arm in ("on", "off"):
        for f in sorted(ROOT.glob(f"exp_{arm}_[0-9]*_t[0-9]*.log")):
            m = re.match(rf"exp_{arm}_(\d+)_t(\d+)\.log$", f.name)
            if not m:
                continue
            lat = int(m.group(1))
            parsed = parse_exp_log(f)
            if parsed:
                data[arm][lat].append(parsed)
    return data


# ---------------------------------------------------------------------------
# Figure 1 — Tripwire sweep
# ---------------------------------------------------------------------------
def fig_tripwire_sweep(data: Dict) -> None:
    common = sorted(l for l in data["on"]
                    if data["on"].get(l) and data["off"].get(l))
    if not common:
        print("  SKIP fig1: no paired latencies found")
        return

    def med_lo_hi(trials):
        vals = [t["ratio"] for t in trials]
        return (float(np.median(vals)),
                float(np.min(vals)),
                float(np.max(vals)))

    xs      = np.array(common, dtype=float)
    on_med, on_lo, on_hi   = zip(*[med_lo_hi(data["on"][l])  for l in common])
    off_med, off_lo, off_hi = zip(*[med_lo_hi(data["off"][l]) for l in common])

    on_med, on_lo, on_hi   = map(np.array, (on_med, on_lo, on_hi))
    off_med, off_lo, off_hi = map(np.array, (off_med, off_lo, off_hi))

    fig, ax = plt.subplots(figsize=(3.4, 2.4))

    ax.fill_between(xs, on_lo, on_hi, color="#1f77b4", alpha=0.15, linewidth=0)
    ax.plot(xs, on_med, "o-", color="#1f77b4", linewidth=1.6,
            markersize=5, label="Tripwire ON")

    ax.fill_between(xs, off_lo, off_hi, color="#d62728", alpha=0.15, linewidth=0)
    ax.plot(xs, off_med, "s--", color="#d62728", linewidth=1.6,
            markersize=5, label="Tripwire OFF")

    # Real log-scale spacing so 60/100/150 don't overlap.
    ax.set_xscale("log")
    ax.set_xticks([1, 10, 30, 60, 100, 150, 250, 500])
    ax.set_xticklabels(["0", "10", "30", "60", "100", "150", "250", "500"],
                       rotation=45, ha="right")

    ax.set_xlabel("Injected fsync latency (ms)")
    ax.set_ylabel("Throughput\n(fraction of warmup)")
    ax.set_ylim(-0.02, 0.70)
    ax.legend(loc="upper right", frameon=True)
    ax.set_title("Tripwire preserves throughput\nunder storage degradation",
                 fontsize=9, pad=6)

    fig.savefig(FIGDIR / "fig1_tripwire_sweep.pdf")
    fig.savefig(FIGDIR / "fig1_tripwire_sweep.png")
    plt.close(fig)
    print("  wrote figures/fig1_tripwire_sweep.pdf/.png")


# ---------------------------------------------------------------------------
# Figure 2 — Tripwire lead time
# ---------------------------------------------------------------------------
def fig_tripwire_lead(data: Dict) -> None:
    latencies = [l for l in sorted(data["on"])
                 if any("tripwire_lead_ms" in t for t in data["on"][l])]
    if not latencies:
        print("  SKIP fig2: no tripwire_lead_ms in any log")
        return

    labels  = []
    points  = []
    medians = []
    for i, lat in enumerate(latencies):
        leads = [t["tripwire_lead_ms"] for t in data["on"][lat]
                 if "tripwire_lead_ms" in t]
        labels.append(str(lat))
        for lead in leads:
            points.append((i, lead))
        medians.append(float(np.median(leads)))

    fig, ax = plt.subplots(figsize=(3.4, 2.2))

    for i, lead in points:
        ax.scatter(i, lead, s=22, alpha=0.55, color="#1f77b4",
                   edgecolors="none", zorder=2)
    ax.bar(range(len(medians)), medians, width=0.35,
           color="#1f77b4", alpha=0.25, zorder=1)

    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels)
    ax.set_xlabel("Injected fsync latency (ms)")
    ax.set_ylabel("Tripwire lead over\nRaft timeout (ms)")
    ax.axhline(0, color="black", linewidth=0.6)
    ax.set_ylim(0, max(350, int(max(medians) * 1.3)))
    ax.set_title("Tripwire fires before Raft election timeout",
                 fontsize=9, pad=6)

    fig.savefig(FIGDIR / "fig2_tripwire_lead.pdf")
    fig.savefig(FIGDIR / "fig2_tripwire_lead.png")
    plt.close(fig)
    print("  wrote figures/fig2_tripwire_lead.pdf/.png")


# ---------------------------------------------------------------------------
# Figure 3 — Ablation tail latency
# ---------------------------------------------------------------------------
def fig_ablation_tail() -> None:
    csv_path = ROOT / "ablation_results.csv"
    if not csv_path.exists():
        print("  SKIP fig3: ablation_results.csv not found")
        return
    rows = list(csv.DictReader(open(csv_path)))
    if not rows:
        print("  SKIP fig3: empty CSV")
        return

    configs   = [r["config"] for r in rows]
    p50_vals  = [float(r["p50_us"])  for r in rows]
    p99_vals  = [float(r["p99_us"])  for r in rows]
    p999_vals = [float(r["p999_us"]) for r in rows]

    x     = np.arange(len(configs))
    width = 0.26

    fig, ax = plt.subplots(figsize=(3.4, 2.4))
    ax.bar(x - width, p50_vals,  width, label="p50",   color="#4c72b0")
    ax.bar(x,         p99_vals,  width, label="p99",   color="#dd8452")
    ax.bar(x + width, p999_vals, width, label="p99.9", color="#55a868")

    ax.set_xticks(x)
    ax.set_xticklabels([c.replace("_", "\n") for c in configs], fontsize=7)
    ax.set_ylabel("Latency (µs)")
    ax.set_yscale("log")
    ax.legend(loc="upper left", frameon=True)
    ax.set_title("Ablation: tail latency across LSM configurations",
                 fontsize=9, pad=6)

    fig.savefig(FIGDIR / "fig3_ablation_tail.pdf")
    fig.savefig(FIGDIR / "fig3_ablation_tail.png")
    plt.close(fig)
    print("  wrote figures/fig3_ablation_tail.pdf/.png")


# ---------------------------------------------------------------------------
# LaTeX tables
# ---------------------------------------------------------------------------
def table_tripwire(data: Dict) -> None:
    latencies = sorted(set(data["on"]) | set(data["off"]))

    lines = [
        r"\begin{table}[t]",
        r"\centering",
        r"\caption{Throughput preservation under injected fsync latency. "
        r"Median over all trials; range in parentheses.}",
        r"\label{tab:tripwire}",
        r"\small",
        r"\begin{tabular}{r r r r r}",
        r"\toprule",
        r"\textbf{Latency} & \textbf{Tripwire ON} & \textbf{Tripwire OFF} "
        r"& \textbf{Ratio} & \textbf{Lead (ms)} \\",
        r"\midrule",
    ]

    for lat in latencies:
        on_trials  = data["on"].get(lat, [])
        off_trials = data["off"].get(lat, [])
        if not on_trials or not off_trials:
            continue

        on_ratios  = [t["ratio"] for t in on_trials]
        off_ratios = [t["ratio"] for t in off_trials]
        leads      = [t["tripwire_lead_ms"] for t in on_trials
                      if "tripwire_lead_ms" in t]

        on_med  = np.median(on_ratios)
        off_med = np.median(off_ratios)
        ratio   = on_med / off_med if off_med > 0 else float("nan")

        def rng(vals):
            return f" ({min(vals):.2f}--{max(vals):.2f})" if len(vals) > 1 else ""

        on_str    = f"{on_med:.3f}{rng(on_ratios)}"
        off_str   = f"{off_med:.3f}{rng(off_ratios)}"
        ratio_str = f"{ratio:.1f}$\\times$" if not np.isnan(ratio) else "--"
        lead_str  = f"{int(np.median(leads))}" if leads else "--"

        lines.append(
            f"{lat}~ms & {on_str} & {off_str} & {ratio_str} & {lead_str} \\\\")

    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}"]
    (FIGDIR / "table1_tripwire.tex").write_text("\n".join(lines))
    print("  wrote figures/table1_tripwire.tex")


def table_ablation() -> None:
    csv_path = ROOT / "ablation_results.csv"
    if not csv_path.exists():
        print("  SKIP table2: ablation_results.csv not found")
        return
    rows = list(csv.DictReader(open(csv_path)))
    lines = [
        r"\begin{table}[t]", r"\centering",
        r"\caption{Single-node ablation: tail latency by LSM configuration.}",
        r"\label{tab:ablation}", r"\small",
        r"\begin{tabular}{l r r r}", r"\toprule",
        r"\textbf{Configuration} & \textbf{p50 (µs)} & \textbf{p99 (µs)} "
        r"& \textbf{p99.9 (µs)} \\", r"\midrule",
    ]
    for r in rows:
        name = r["config"].replace("_", r"\_")
        lines.append(
            f"{name} & {r['p50_us']} & {r['p99_us']} & {r['p999_us']} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}"]
    (FIGDIR / "table2_ablation.tex").write_text("\n".join(lines))
    print("  wrote figures/table2_ablation.tex")


def table_threshold() -> None:
    csv_path = ROOT / "threshold_sensitivity.csv"
    if not csv_path.exists():
        print("  SKIP table3: threshold_sensitivity.csv not found")
        return
    rows = list(csv.DictReader(open(csv_path)))
    lines = [
        r"\begin{table}[t]", r"\centering",
        r"\caption{Tripwire threshold sensitivity.}",
        r"\label{tab:threshold}", r"\small",
        r"\begin{tabular}{r r r r}", r"\toprule",
        r"\textbf{Offset} & \textbf{Throughput (wps)} & \textbf{p99 (µs)} "
        r"& \textbf{Leader transfers} \\", r"\midrule",
    ]
    for r in rows:
        lines.append(f"{r['tripwire_offset']} & {r['throughput_wps']} & "
                     f"{r['p99_us']} & {r['leader_transfers']} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}"]
    (FIGDIR / "table3_threshold.tex").write_text("\n".join(lines))
    print("  wrote figures/table3_threshold.tex")


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def main() -> None:
    print("[plot] collecting sweep data ...")
    data = collect_sweep_data()

    for arm in ("on", "off"):
        print(f"  {arm}:")
        for lat in sorted(data[arm]):
            print(f"    {lat:>4} ms  trials={len(data[arm][lat])}")

    print("[plot] generating figures ...")
    fig_tripwire_sweep(data)
    fig_tripwire_lead(data)
    fig_ablation_tail()

    print("[plot] generating LaTeX tables ...")
    table_tripwire(data)
    table_ablation()
    table_threshold()

    print(f"[plot] done. outputs in {FIGDIR}")


if __name__ == "__main__":
    main()