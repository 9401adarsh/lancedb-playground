#!/usr/bin/env python3
"""
Parse LanceDB benchmark log files and summarize per-query latencies.

Usage:
    python3 scripts/plot.py logs/global_session_*.log logs/db_session_*.log
    python3 scripts/plot.py logs/              # all .log files in directory
"""

import re
import sys
import os
import statistics
from collections import defaultdict
from pathlib import Path


# Matches lines like:
# [global_session] [thread 0] ./data/db2/::table_2 => 1 batches, 10 rows in 4.168333ms
SEARCH_LINE_RE = re.compile(
    r"\[(?P<mode>\w+)\]\s+\[thread\s+(?P<thread>\d+)\]\s+"
    r"(?P<table>\S+)\s+=>\s+\d+\s+batches,\s+\d+\s+rows\s+in\s+"
    r"(?P<latency_ms>[\d.]+)ms"
)

# Matches iteration header: ========== Iteration 3/5 ==========
ITER_RE = re.compile(r"=+\s+Iteration\s+(\d+)/(\d+)\s+=+")

# Matches iteration footer: ---------- Iteration 3/5 completed in 8.292916ms ----------
ITER_DONE_RE = re.compile(
    r"-+\s+Iteration\s+(\d+)/(\d+)\s+completed\s+in\s+([\d.]+)(ms|s)\s+-+"
)

# Matches config line: Running in mode: GlobalSession, iterations: 5, threads: 4
CONFIG_RE = re.compile(
    r"Running in mode:\s+(\w+),\s+iterations:\s+(\d+),\s+threads:\s+(\d+)"
)


def parse_log_file(filepath: str) -> dict:
    """Parse a single log file and extract latencies + metadata."""
    latencies = []  # list of (iteration, thread_id, table, latency_ms)
    iter_times = []  # list of (iteration, elapsed_ms)
    mode = None
    iterations = None
    threads = None

    with open(filepath) as f:
        current_iter = 0
        for line in f:
            # Parse config
            m = CONFIG_RE.search(line)
            if m:
                mode = m.group(1)
                iterations = int(m.group(2))
                threads = int(m.group(3))
                continue

            # Parse iteration header
            m = ITER_RE.search(line)
            if m:
                current_iter = int(m.group(1))
                continue

            # Parse search result
            m = SEARCH_LINE_RE.search(line)
            if m:
                latencies.append((
                    current_iter,
                    int(m.group("thread")),
                    m.group("table"),
                    float(m.group("latency_ms")),
                ))
                continue

            # Parse iteration completion
            m = ITER_DONE_RE.search(line)
            if m:
                elapsed = float(m.group(3))
                unit = m.group(4)
                if unit == "s":
                    elapsed *= 1000.0  # convert to ms
                iter_times.append((int(m.group(1)), elapsed))

    return {
        "file": filepath,
        "mode": mode,
        "iterations": iterations,
        "threads": threads,
        "latencies": latencies,
        "iter_times": iter_times,
    }


def percentile(data: list, p: float) -> float:
    """Calculate the p-th percentile of a sorted list."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def print_summary(parsed: dict):
    """Print a formatted summary of a parsed log file."""
    mode = parsed["mode"] or "unknown"
    threads = parsed["threads"] or "?"
    iterations = parsed["iterations"] or "?"
    latencies = parsed["latencies"]
    iter_times = parsed["iter_times"]

    if not latencies:
        print(f"  No search results found in {parsed['file']}")
        return

    all_ms = [lat for (_, _, _, lat) in latencies]

    print(f"\n{'='*70}")
    print(f"  File: {os.path.basename(parsed['file'])}")
    print(f"  Mode: {mode}  |  Threads: {threads}  |  Iterations: {iterations}")
    print(f"  Total queries: {len(all_ms)}")
    print(f"{'='*70}")

    # Overall latency stats
    print(f"\n  {'Query Latency (ms)':>30}")
    print(f"  {'─'*40}")
    print(f"  {'Mean':>20}: {statistics.mean(all_ms):>10.3f}")
    print(f"  {'Median (p50)':>20}: {percentile(all_ms, 50):>10.3f}")
    print(f"  {'p95':>20}: {percentile(all_ms, 95):>10.3f}")
    print(f"  {'p99':>20}: {percentile(all_ms, 99):>10.3f}")
    print(f"  {'Min':>20}: {min(all_ms):>10.3f}")
    print(f"  {'Max':>20}: {max(all_ms):>10.3f}")
    print(f"  {'Std Dev':>20}: {statistics.stdev(all_ms) if len(all_ms) > 1 else 0:>10.3f}")

    # Per-iteration breakdown
    if iter_times:
        print(f"\n  {'Iteration Elapsed (ms)':>30}")
        print(f"  {'─'*40}")
        for (i, elapsed) in iter_times:
            # count queries in this iteration
            n_queries = sum(1 for (it, _, _, _) in latencies if it == i)
            iter_lats = [lat for (it, _, _, lat) in latencies if it == i]
            avg = statistics.mean(iter_lats) if iter_lats else 0
            print(f"  Iter {i:>3}: {elapsed:>10.3f} ms  ({n_queries} queries, avg {avg:.3f} ms)")

    # Per-iteration latency comparison (skip iter 1 as warmup)
    warmup_lats = [lat for (it, _, _, lat) in latencies if it == 1]
    steady_lats = [lat for (it, _, _, lat) in latencies if it > 1]

    if warmup_lats and steady_lats:
        print(f"\n  {'Warmup vs Steady-State':>30}")
        print(f"  {'─'*40}")
        print(f"  {'Warmup (iter 1) avg':>20}: {statistics.mean(warmup_lats):>10.3f} ms")
        print(f"  {'Steady-state avg':>20}: {statistics.mean(steady_lats):>10.3f} ms")
        speedup = statistics.mean(warmup_lats) / statistics.mean(steady_lats)
        print(f"  {'Speedup':>20}: {speedup:>10.2f}x")

    print()


def print_comparison(all_parsed: list):
    """Print a side-by-side comparison table across multiple log files."""
    # Filter to files that have data
    valid = [p for p in all_parsed if p["latencies"]]
    if len(valid) < 2:
        return

    print(f"\n{'='*70}")
    print(f"  COMPARISON ACROSS MODES")
    print(f"{'='*70}")

    header = f"  {'Mode':<20} {'Threads':>7} {'Queries':>8} {'Mean':>8} {'p50':>8} {'p95':>8} {'p99':>8}"
    print(header)
    print(f"  {'─'*69}")

    for p in sorted(valid, key=lambda x: (x["mode"] or "", x["threads"] or 0)):
        all_ms = [lat for (_, _, _, lat) in p["latencies"]]
        # Use only steady-state (skip iter 1)
        steady = [lat for (it, _, _, lat) in p["latencies"] if it > 1]
        data = steady if steady else all_ms

        mode = p["mode"] or "?"
        threads = p["threads"] or "?"
        print(
            f"  {mode:<20} {threads:>7} {len(data):>8} "
            f"{statistics.mean(data):>8.3f} {percentile(data, 50):>8.3f} "
            f"{percentile(data, 95):>8.3f} {percentile(data, 99):>8.3f}"
        )

    print(f"\n  (Comparison uses steady-state latencies only, skipping iteration 1 warmup)")
    print()


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/plot.py <log_file_or_dir> [...]")
        print("  Pass one or more .log files, or a directory to scan.")
        sys.exit(1)

    # Collect all log file paths
    log_files = []
    for arg in sys.argv[1:]:
        p = Path(arg)
        if p.is_dir():
            log_files.extend(sorted(p.glob("*.log")))
        elif p.is_file():
            log_files.append(p)
        else:
            print(f"Warning: '{arg}' not found, skipping.")

    if not log_files:
        print("No log files found.")
        sys.exit(1)

    # Parse and summarize each file
    all_parsed = []
    for lf in log_files:
        parsed = parse_log_file(str(lf))
        all_parsed.append(parsed)
        print_summary(parsed)

    # Print comparison if multiple files
    if len(all_parsed) > 1:
        print_comparison(all_parsed)


if __name__ == "__main__":
    main()
