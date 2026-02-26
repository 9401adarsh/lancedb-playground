## Session Cache Benchmarking for Lance DB Rust SDK

This repository benchmarks LanceDB session cache strategies using the Rust SDK. It studies the performance differences between:

- **No explicit session** — letting the library manage its own default session
- **Global session** — one shared session across all DBs
- **DB session** — one session per database
- **Table session** — one session per (database, table) pair

## Prerequisites

- [Rust toolchain](https://rustup.rs/) (`cargo`)

## Quick Start

### 1. Seed the databases

Create test data in `./data/db1/` and `./data/db2/` (directories are created automatically):

```bash
# Seed with default 10,000 rows per table
cargo run -- seed

# Or specify a custom row count
NUM_SEED_ROWS=50000 cargo run -- seed
```

### 2. Run benchmarks

```bash
cargo run -- <mode> [iterations]
```

| Argument | Description |
|---|---|
| `mode` | **Required.** One of: `no_session`, `global_session`, `db_session`, `table_session` |
| `iterations` | *Optional.* Number of times to run the test (default: `1`) |

#### Examples

```bash
# Run no_session mode once
cargo run -- no_session

# Run global_session mode 5 times
cargo run -- global_session 5

# Run db_session mode 3 times
cargo run -- db_session 3

# Run table_session mode 10 times
cargo run -- table_session 10
```

### 3. View logs

All benchmark output is logged to both stdout and a timestamped file in `./logs/`:

```
logs/no_session_20260226_154336.log
logs/global_session_20260226_155012.log
```

## Modes

| Mode | CLI arg | Description |
|---|---|---|
| **Seed** | `seed` | Populates `db1` and `db2` with test tables (`table_1`, `table_2`). Run once before benchmarking. |
| **No Session** | `no_session` | Connects without an explicit session — the library creates its own default internally. |
| **Global Session** | `global_session` | Creates one session shared across all databases. |
| **DB Session** | `db_session` | Creates a separate session for each database. |
| **Table Session** | `table_session` | Creates a separate session for each (database, table) pair. |

## Project Structure

```
src/
├── main.rs    # CLI parsing, test mode orchestration, iteration loop
├── utils.rs   # Session/connection setup, index management, vector search
├── seed.rs    # Test data generation (Arrow RecordBatch)
└── log.rs     # Tee-logging to stdout + file
```