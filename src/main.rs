mod log;
mod seed;
mod utils;
use lancedb::index::Index;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::vec::Vec;
use tokio::task::JoinSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Seed,
    NoSession,
    GlobalSession,
    DBSession,
    TableSession,
}

struct CliArgs {
    mode: Mode,
    iterations: usize,
    threads: usize,
}

fn parse_args() -> CliArgs {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!(
            "Usage: {} <mode> [iterations] [threads]\n  Modes: seed, no_session, global_session, db_session, table_session\n  Iterations: number of times to run the test (default: 1, ignored for seed)\n  Threads: number of concurrent search streams (default: 1)",
            args[0]
        );
    }
    let mode = match args[1].as_str() {
        "seed" => Mode::Seed,
        "no_session" => Mode::NoSession,
        "global_session" => Mode::GlobalSession,
        "db_session" => Mode::DBSession,
        "table_session" => Mode::TableSession,
        _ => {
            panic!("Invalid mode '{}'. Use: seed, no_session, global_session, db_session, or table_session", args[1])
        }
    };

    let iterations = if args.len() >= 3 {
        args[2].parse::<usize>().unwrap_or_else(|_| {
            panic!("Invalid iterations '{}': must be a positive integer", args[2])
        })
    } else {
        1
    };

    let threads = if args.len() >= 4 {
        args[3].parse::<usize>().unwrap_or_else(|_| {
            panic!("Invalid threads '{}': must be a positive integer", args[3])
        })
    } else {
        1
    };

    if iterations == 0 {
        panic!("Iterations must be >= 1");
    }
    if threads == 0 {
        panic!("Threads must be >= 1");
    }

    CliArgs { mode, iterations, threads }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Seed => "seed",
        Mode::NoSession => "no_session",
        Mode::GlobalSession => "global_session",
        Mode::DBSession => "db_session",
        Mode::TableSession => "table_session",
    }
}

/// Result of a single concurrent search query.
struct SearchResult {
    thread_id: usize,
    search_order: usize, // 0-indexed position within this thread's sequence
    label: String,
    num_batches: usize,
    num_rows: usize,
    elapsed: Duration,
}

/// Run concurrent vector searches with T independent worker threads.
/// Each worker sequentially searches all tables (simulating a real client).
/// T workers run concurrently, so at any moment there are up to T searches in flight.
async fn run_concurrent_searches(
    tables: &[(String, lancedb::Table)],
    num_threads: usize,
) -> Vec<SearchResult> {
    let tables = Arc::new(tables.to_vec());
    let mut join_set: JoinSet<Vec<SearchResult>> = JoinSet::new();

    for thread_id in 0..num_threads {
        let tables = tables.clone();
        join_set.spawn(async move {
            let mut results = Vec::new();
            // Shuffle table order per worker so workers hit different tables
            // at different times, creating realistic cross-table contention
            let mut table_order: Vec<usize> = (0..tables.len()).collect();
            {
                use rand::seq::SliceRandom;
                let mut rng = rand::rng();
                table_order.shuffle(&mut rng);
            }
            for (search_order, idx) in table_order.into_iter().enumerate() {
                let (label, table) = &tables[idx];
                let qvec = utils::random_query_vec(64);
                let t0 = Instant::now();
                let batches = utils::run_nearest_to_vector_search_k(table, qvec, "vector", 10)
                    .await
                    .unwrap_or_else(|e| {
                        panic!("vector search failed on {} (thread {}): {}", label, thread_id, e)
                    });
                let elapsed = t0.elapsed();
                results.push(SearchResult {
                    thread_id,
                    search_order,
                    label: label.clone(),
                    num_batches: batches.len(),
                    num_rows: batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                    elapsed,
                });
            }
            results
        });
    }

    let mut all_results = Vec::new();
    while let Some(res) = join_set.join_next().await {
        all_results.extend(res.expect("search worker panicked"));
    }
    // Sort by thread_id then execution order (preserves actual search sequence)
    all_results.sort_by(|a, b| a.thread_id.cmp(&b.thread_id).then(a.search_order.cmp(&b.search_order)));
    all_results
}

// ---------------------------------------------------------------------------
// Setup functions — called ONCE before the iteration loop
// ---------------------------------------------------------------------------

/// Holds the state set up once and reused across all iterations.
struct BenchmarkState {
    /// (label, table) pairs to search
    tables: Vec<(String, lancedb::Table)>,
    /// Sessions to report stats on (label, session)
    sessions: Vec<(String, Arc<lancedb::Session>)>,
}

async fn setup_global_session(
    uris: &[String],
    table_names: &[String],
) -> BenchmarkState {
    log_println!("[global_session] Setting up one shared Session across all DBs...");
    let (session, connections) = utils::setup_global_session(uris, None, None)
        .await
        .expect("failed to setup global session");
    log_println!("[global_session] Session: {:?}", session);

    let mut tables = Vec::new();
    for (uri, conn) in connections.iter() {
        for table_name in table_names.iter() {
            let table = conn
                .open_table(table_name)
                .execute()
                .await
                .unwrap_or_else(|e| {
                    panic!("failed to open table '{}' at {}: {}", table_name, uri, e)
                });
            let _ = utils::ensure_vector_index(&table, table_name, "vector", Index::Auto).await;
            tables.push((format!("{}::{}", uri, table_name), table));
        }
    }

    BenchmarkState {
        tables,
        sessions: vec![("global".to_string(), session)],
    }
}

async fn setup_db_session(
    uris: &[String],
    table_names: &[String],
) -> BenchmarkState {
    log_println!("[db_session] Setting up one Session per DB...");
    let pool = utils::setup_db_session(uris, None, None)
        .await
        .expect("failed to setup db session pool");

    let mut tables = Vec::new();
    let mut sessions = Vec::new();
    for (uri, (session, conn)) in pool.iter() {
        sessions.push((uri.clone(), session.clone()));
        for table_name in table_names.iter() {
            let table = conn
                .open_table(table_name)
                .execute()
                .await
                .unwrap_or_else(|e| {
                    panic!("failed to open table '{}' at {}: {}", table_name, uri, e)
                });
            let _ = utils::ensure_vector_index(&table, table_name, "vector", Index::Auto).await;
            tables.push((format!("{}::{}", uri, table_name), table));
        }
    }

    BenchmarkState { tables, sessions }
}

async fn setup_table_session(
    uris: &[String],
    table_names: &[String],
) -> BenchmarkState {
    log_println!("[table_session] Setting up one Session per (DB, table)...");
    let mut pool: utils::TableSessionPool = std::collections::HashMap::new();
    for uri in uris.iter() {
        for table_name in table_names.iter() {
            utils::add_table_session_to_pool(&mut pool, uri, table_name, None, None)
                .await
                .unwrap_or_else(|e| {
                    panic!("failed to add table session {}::{}: {}", uri, table_name, e)
                });
        }
    }

    let mut tables = Vec::new();
    let mut sessions = Vec::new();
    for (key, (session, _conn, table)) in pool.iter() {
        let _ = utils::ensure_vector_index(table, key, "vector", Index::Auto).await;
        tables.push((key.clone(), table.clone()));
        sessions.push((key.clone(), session.clone()));
    }

    BenchmarkState { tables, sessions }
}

async fn setup_no_session(
    uris: &[String],
    table_names: &[String],
) -> BenchmarkState {
    log_println!("[no_session] Connecting without an explicit session (library default)...");
    let connections = utils::setup_no_session(uris)
        .await
        .expect("failed to setup no-session connections");

    let mut tables = Vec::new();
    for (uri, conn) in connections.iter() {
        for table_name in table_names.iter() {
            let table = conn
                .open_table(table_name)
                .execute()
                .await
                .unwrap_or_else(|e| {
                    panic!("failed to open table '{}' at {}: {}", table_name, uri, e)
                });
            let _ = utils::ensure_vector_index(&table, table_name, "vector", Index::Auto).await;
            tables.push((format!("{}::{}", uri, table_name), table));
        }
    }

    BenchmarkState {
        tables,
        sessions: vec![], // no explicit session to report on
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cli = parse_args();
    let mode = cli.mode;
    let n = cli.iterations;
    let threads = cli.threads;

    // Initialize logging to file (for non-seed modes)
    if mode != Mode::Seed {
        match log::init_log(mode_name(mode)) {
            Ok(path) => log_println!("Logging to: {}", path.display()),
            Err(e) => eprintln!("Warning: could not initialize log file: {}", e),
        }
    }

    log_println!("Running in mode: {:?}, iterations: {}, threads: {}", mode, n, threads);

    let uris: Vec<String> = vec!["./data/db1/".to_string(), "./data/db2/".to_string()];
    let table_names: Vec<String> = vec!["table_1".to_string(), "table_2".to_string()];

    if mode == Mode::Seed {
        let num_rows = env::var("NUM_SEED_ROWS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10000);
        println!("Generating seed data with {} rows...", num_rows);
        let session = utils::make_session(None, None);
        println!("Session created: {:?}", session);

        let mut connections = utils::create_connection_pool();
        for uri in uris.iter() {
            utils::add_connection_to_pool(&mut connections, session.clone(), uri)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Failed to connect to {}: Error: {}", uri, e);
                });
        }
        println!("Connections established: {:?}", connections.keys());
        for (uri, connection) in connections.iter() {
            for table_name in table_names.iter() {
                println!(
                    "Creating table '{}' in database at URI: {}",
                    table_name, uri
                );
                match utils::open_or_create_seeded_table(connection, table_name, num_rows).await {
                    Ok(table) => {
                        println!("Table details: {:?}", table);
                        utils::ensure_vector_index(&table, table_name, "vector", Index::Auto).await.unwrap_or_else(|e| {
                            eprintln!(
                                "Failed to create index on table '{}' in database at URI: {}: Error: {}",
                                table_name, uri, e
                            );
                        });
                    }
                    Err(e) => eprintln!(
                        "Failed to create table '{}' in database at URI: {}: Error: {}",
                        table_name, uri, e
                    ),
                }
            }
        }
        println!("Seeding complete. Exiting.");
        return;
    }

    // ===== Setup once =====
    let tag = mode_name(mode);
    let setup_start = Instant::now();
    let state = match mode {
        Mode::NoSession => setup_no_session(&uris, &table_names).await,
        Mode::GlobalSession => setup_global_session(&uris, &table_names).await,
        Mode::DBSession => setup_db_session(&uris, &table_names).await,
        Mode::TableSession => setup_table_session(&uris, &table_names).await,
        Mode::Seed => unreachable!(),
    };
    let setup_elapsed = setup_start.elapsed();
    log_println!("[{}] Setup completed in {:?} ({} tables ready)", tag, setup_elapsed, state.tables.len());

    // Log initial session stats
    for (label, session) in &state.sessions {
        log_println!("[{}] Initial session stats for {}:", tag, label);
        utils::get_session_stats(session).await;
    }

    // ===== Benchmark loop (search only) =====
    let total_start = Instant::now();
    for i in 1..=n {
        log_println!("========== Iteration {}/{} ==========", i, n);
        let iter_start = Instant::now();

        let total_tasks = state.tables.len() * threads;
        log_println!("[{}] Launching {} searches ({} threads x {} tables)...", tag, total_tasks, threads, state.tables.len());
        let results = run_concurrent_searches(&state.tables, threads).await;

        for r in &results {
            log_println!(
                "[{}] [thread {}] {} => {} batches, {} rows in {:?}",
                tag, r.thread_id, r.label, r.num_batches, r.num_rows, r.elapsed
            );
        }

        // Session stats after each iteration (shows cache warming across iterations)
        for (label, session) in &state.sessions {
            log_println!("[{}] Session stats for {} after iteration {}:", tag, label, i);
            utils::get_session_stats(session).await;
        }

        let iter_elapsed = iter_start.elapsed();
        log_println!("---------- Iteration {}/{} completed in {:?} ----------", i, n, iter_elapsed);
    }

    let total_elapsed = total_start.elapsed();
    log_println!("========================================");
    log_println!(
        "All {} iterations of {:?} completed in {:?}",
        n,
        mode,
        total_elapsed
    );
    log_println!("========================================");
}
