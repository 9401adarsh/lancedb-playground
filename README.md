## Session Cache Benchmarking for Lance DB Rust SDK

This repository benchmarks LanceDB session cache strategies using the Rust SDK. The goal is to determine whether a single shared session causes cache contention under concurrent load, or whether it's safe to share one session across all databases.

### Session Strategies Under Test

- **No Session** — no explicit session; the library creates its own default internally per connection
- **Global Session** — one shared session (and its cache) across all databases
- **DB Session** — one session per database
- **Table Session** — one session per (database, table) pair

## Prerequisites

- [Rust toolchain](https://rustup.rs/) (`cargo`)

## Quick Start

### 1. Seed the databases

Create test data in `./data/db1/` and `./data/db2/`:

```bash
# Seed with default 10,000 rows per table
cargo run -- seed

# Or specify a custom row count
NUM_SEED_ROWS=50000 cargo run -- seed
```

### 2. Run benchmarks

```bash
cargo run -- <mode> [iterations] [threads]
```

| Argument | Description |
|---|---|
| `mode` | **Required.** One of: `no_session`, `global_session`, `db_session`, `table_session` |
| `iterations` | *Optional.* Number of search-only benchmark iterations (default: `1`) |
| `threads` | *Optional.* Number of concurrent worker threads per iteration (default: `1`) |

#### Examples

```bash
# Single-threaded, one iteration
cargo run -- global_session

# 10 iterations, 4 concurrent workers
cargo run -- global_session 10 4

# Compare modes at same concurrency level
cargo run -- global_session 20 8
cargo run -- db_session 20 8
cargo run -- no_session 20 8
```

### 3. View logs

All output is logged to both stdout and a timestamped file in `./logs/`:

```
logs/global_session_20260301_014540.log
logs/db_session_20260301_014812.log
```

## How the Benchmark Works

Each benchmark run has **two distinct phases**:

### Phase 1: Setup (once)

Runs exactly once before the iteration loop:
- Create session(s) and connection(s) per the selected mode
- Open all tables
- Ensure vector indexes exist (this is the slow part: ~10-15s per table)
- Log initial (cold) cache stats

### Phase 2: Benchmark loop (N iterations)

Each iteration **only runs search queries** against the already-warm session:
- `T` independent worker threads are spawned simultaneously
- Each worker sequentially searches all tables in a **randomized order**
- At any moment, up to `T` searches are in flight, all hitting the session cache
- Per-query latency, batch count, and row count are logged per thread
- Session cache stats (hits, misses, entries, size) are logged after each iteration

```
Worker 0: search(db2::t1) → search(db1::t2) → search(db1::t1) → search(db2::t2)
Worker 1: search(db1::t1) → search(db2::t2) → search(db2::t1) → search(db1::t2)
Worker 2: search(db2::t2) → search(db1::t1) → search(db1::t2) → search(db2::t1)
           ↑ all workers run concurrently, each doing serial searches in shuffled order
```

<!-- The cache **stays warm across iterations** — misses only happen on the first iteration, subsequent iterations are pure cache hits. This is intentional: we're measuring steady-state concurrent access performance, not cold-start behavior. -->

<!-- ## What We're Measuring

The core question (from Yuval): *does a shared global session cause cache contention under concurrent access?*

The session's cache is backed by `moka::future::Cache`, which uses lock-free internal sharding. In theory, reads don't contend. This benchmark tests that empirically by comparing per-query latencies:

| Comparison | What it tells you |
|---|---|
| `global_session` vs `db_session` at same thread count | Does sharing one cache across DBs add latency? |
| Same mode at 1 vs 4 vs 8 threads | Does increasing concurrency degrade per-query latency? |
| Cache hit ratios across modes | Does a shared cache give higher hit rates (offsetting any contention)? | -->

## Modes

| Mode | CLI arg | Description |
|---|---|---|
| **Seed** | `seed` | Populates `db1` and `db2` with test tables (`table_1`, `table_2`). Run once before benchmarking. |
| **No Session** | `no_session` | Connects without an explicit session — the library creates its own default internally. |
| **Global Session** | `global_session` | Creates one session shared across all databases. |
| **DB Session** | `db_session` | Creates a separate session for each database. |
| **Table Session** | `table_session` | Creates a separate session for each (database, table) pair. |

## Analysing Results

Use `scripts/summarise_logs.py` to parse log files and generate latency statistics. No dependencies required — pure Python stdlib.

### Usage

```bash
# Summarise a single log file
python3 scripts/summarise_logs.py logs/global_session_20260301_021917.log

# Compare multiple modes side-by-side
python3 scripts/summarise_logs.py logs/global_session_*.log logs/db_session_*.log

# Summarise all log files in a directory
python3 scripts/summarise_logs.py logs/
```

### What it outputs

**Per-file summary:**
- Query latency stats: mean, median (p50), p95, p99, min, max, std dev
- Per-iteration breakdown: wall clock time, query count, average latency
- Warmup vs steady-state comparison (iteration 1 vs rest)

**Cross-mode comparison table** (when multiple files are provided):
```
Mode                 Threads  Queries     Mean      p50      p95      p99
─────────────────────────────────────────────────────────────────────
TableSession               8      288    3.007    2.913    4.546    5.541
GlobalSession              8      288    3.095    2.988    5.175    5.888
NoSession                  8      288    3.100    3.011    4.758    5.671
DBSession                  8      288    3.256    3.148    5.179    5.779

(Comparison uses steady-state latencies only, skipping iteration 1 warmup)
```

## Project Structure

```
src/
├── main.rs    # CLI, setup-once / benchmark-loop orchestration, concurrent search runner
├── utils.rs   # Session/connection setup, index management, vector search
├── seed.rs    # Test data generation (Arrow RecordBatch)
└── log.rs     # Tee-logging to stdout + file

scripts/
└── summarise_logs.py  # Parse log files, output latency stats and cross-mode comparisons
```