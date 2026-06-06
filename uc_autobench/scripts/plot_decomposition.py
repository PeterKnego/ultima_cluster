#!/usr/bin/env python3
"""Overlay latency-vs-throughput curves for UC configs + the Aeron IPC floor,
and render a per-layer decomposition bar at a fixed offered load.

Usage: plot_decomposition.py bench-out/*.csv --out-dir bench-out/plots
"""
import argparse, glob, os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def load(paths):
    frames = []
    for pat in paths:
        for p in glob.glob(pat):
            frames.append(pd.read_csv(p))
    if not frames:
        raise SystemExit("no CSVs matched")
    return pd.concat(frames, ignore_index=True)

def curve(df, out):
    fig, ax = plt.subplots(figsize=(9, 6))
    # Only plot rows with a real achieved throughput (Aeron floor rows have 0).
    cdf = df[df["achieved_rate"] > 0]
    for (system, config, inflight), g in cdf.groupby(["system", "config", "inflight"]):
        g = g.sort_values("achieved_rate")
        ax.plot(g["achieved_rate"], g["p99_ns"] / 1e6,
                marker="o", label=f"{system}/{config} if={inflight}")
    # Aeron floor as horizontal reference lines (unloaded, no throughput axis).
    for (system, config), g in df[df["achieved_rate"] == 0].groupby(["system", "config"]):
        ax.axhline(g["p99_ns"].min() / 1e6, ls="--", lw=1, alpha=0.6,
                   label=f"{system}/{config} floor (unloaded)")
    ax.set_xlabel("Achieved throughput (msgs/s)")
    ax.set_ylabel("p99 latency (ms)")
    ax.set_yscale("log"); ax.set_xscale("log")
    ax.set_title("Latency vs throughput: UC commit path vs Aeron IPC")
    ax.legend(fontsize=7); ax.grid(True, which="both", alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(out, "latency_vs_throughput.png"), dpi=130)

def decomposition(df, out):
    # p99 at the lowest target_rate (unloaded) per config, in ms — the layer floor.
    base = (df.sort_values("target_rate")
              .groupby(["system", "config"], as_index=False).first())
    fig, ax = plt.subplots(figsize=(9, 5))
    labels = base["system"] + "/" + base["config"]
    ax.bar(labels, base["p99_ns"] / 1e6)
    ax.set_ylabel("Unloaded p99 (ms)"); ax.set_yscale("log")
    ax.set_title("Per-layer floor (unloaded p99)")
    for t in ax.get_xticklabels():
        t.set_rotation(30); t.set_ha("right")
    fig.tight_layout(); fig.savefig(os.path.join(out, "decomposition.png"), dpi=130)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csvs", nargs="+")
    ap.add_argument("--out-dir", default="bench-out/plots")
    a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)
    df = load(a.csvs)
    curve(df, a.out_dir)
    decomposition(df, a.out_dir)
    print(f"wrote plots to {a.out_dir}")

if __name__ == "__main__":
    main()
