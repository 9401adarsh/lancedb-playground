mod log;
mod seed;
mod utils;
use lancedb::index::Index;
use std::env;
use std::time::Instant;
use std::vec::Vec;

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
}

fn parse_args() -> CliArgs {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!(
            "Usage: {} <mode> [iterations]\n  Modes: seed, no_session, global_session, db_session, table_session\n  Iterations: number of times to run the test (default: 1, ignored for seed)",
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

    if iterations == 0 {
        panic!("Iterations must be >= 1");
    }

    CliArgs { mode, iterations }
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

async fn perform_global_session_test() {
    let uris: Vec<String> = vec!["./data/db1/".to_string(), "./data/db2/".to_string()];
    let table_names: Vec<String> = vec!["table_1".to_string(), "table_2".to_string()];

    log_println!("[global_session] Setting up one shared Session across all DBs...");
    let (session, connections) = utils::setup_global_session(&uris, None, None)
        .await
        .expect("failed to setup global session");
    log_println!("[global_session] Session: {:?}", session);

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

            let qvec = utils::random_query_vec(64);

            let t0 = Instant::now();
            let batches = utils::run_nearest_to_vector_search_k(&table, qvec, "vector", 10)
                .await
                .unwrap_or_else(|e| {
                    panic!("vector search failed on {}::{}: {}", uri, table_name, e)
                });
            let elapsed = t0.elapsed();

            log_println!(
                "[global_session] {}::{} => {} batches, {} rows in {:?}",
                uri,
                table_name,
                batches.len(),
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                elapsed
            );
            log_println!("[global_session] After Search: Session: {:?}", session);
            log_println!("[global_session] After Search: Session Stats:");
            utils::get_session_stats(&session).await;
        }
    }
}

async fn perform_db_session_test() {
    let uris: Vec<String> = vec!["./data/db1/".to_string(), "./data/db2/".to_string()];
    let table_names: Vec<String> = vec!["table_1".to_string(), "table_2".to_string()];

    log_println!("[db_session] Setting up one Session per DB...");
    let pool = utils::setup_db_session(&uris, None, None)
        .await
        .expect("failed to setup db session pool");

    for (uri, (_session, conn)) in pool.iter() {
        for table_name in table_names.iter() {
            let table = conn
                .open_table(table_name)
                .execute()
                .await
                .unwrap_or_else(|e| {
                    panic!("failed to open table '{}' at {}: {}", table_name, uri, e)
                });

            let _ = utils::ensure_vector_index(&table, table_name, "vector", Index::Auto).await;
            let qvec = utils::random_query_vec(64);

            let t0 = Instant::now();
            let batches = utils::run_nearest_to_vector_search_k(&table, qvec, "vector", 10)
                .await
                .unwrap_or_else(|e| {
                    panic!("vector search failed on {}::{}: {}", uri, table_name, e)
                });
            let elapsed = t0.elapsed();

            log_println!(
                "[db_session] {}::{} => {} batches, {} rows in {:?}",
                uri,
                table_name,
                batches.len(),
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                elapsed
            );
            log_println!(
                "[db_session] After Search: Session for {}: {:?}",
                uri, _session
            );
            log_println!("[db_session] After Search: Session Stats:");
            utils::get_session_stats(&_session).await;
        }
    }
}

async fn perform_table_session_test() {
    let uris: Vec<String> = vec!["./data/db1/".to_string(), "./data/db2/".to_string()];
    let table_names: Vec<String> = vec!["table_1".to_string(), "table_2".to_string()];

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

    for (key, (_session, _conn, table)) in pool.iter() {
        let _ = utils::ensure_vector_index(table, key, "vector", Index::Auto).await;
        let qvec = utils::random_query_vec(64);

        let t0 = Instant::now();
        let batches = utils::run_nearest_to_vector_search_k(table, qvec, "vector", 10)
            .await
            .unwrap_or_else(|e| panic!("vector search failed on {}: {}", key, e));
        let elapsed = t0.elapsed();

        log_println!(
            "[table_session] {} => {} batches, {} rows in {:?}",
            key,
            batches.len(),
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            elapsed
        );

        log_println!(
            "[table_session] After Search: Session for {}: {:?}",
            key, _session
        );
        log_println!("[table_session] After Search: Session Stats:");
        utils::get_session_stats(&_session).await;
    }
}

async fn perform_no_session_test() {
    let uris: Vec<String> = vec!["./data/db1/".to_string(), "./data/db2/".to_string()];
    let table_names: Vec<String> = vec!["table_1".to_string(), "table_2".to_string()];

    log_println!("[no_session] Connecting without an explicit session (library default)...");
    let connections = utils::setup_no_session(&uris)
        .await
        .expect("failed to setup no-session connections");

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

            let qvec = utils::random_query_vec(64);

            let t0 = Instant::now();
            let batches = utils::run_nearest_to_vector_search_k(&table, qvec, "vector", 10)
                .await
                .unwrap_or_else(|e| {
                    panic!("vector search failed on {}::{}: {}", uri, table_name, e)
                });
            let elapsed = t0.elapsed();

            log_println!(
                "[no_session] {}::{} => {} batches, {} rows in {:?}",
                uri,
                table_name,
                batches.len(),
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                elapsed
            );
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = parse_args();
    let mode = cli.mode;
    let n = cli.iterations;

    // Initialize logging to file (for non-seed modes)
    if mode != Mode::Seed {
        match log::init_log(mode_name(mode)) {
            Ok(path) => log_println!("Logging to: {}", path.display()),
            Err(e) => eprintln!("Warning: could not initialize log file: {}", e),
        }
    }

    log_println!("Running in mode: {:?}, iterations: {}", mode, n);

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

    // Run the selected test mode N times
    let total_start = Instant::now();
    for i in 1..=n {
        log_println!("========== Iteration {}/{} ==========", i, n);
        let iter_start = Instant::now();

        match mode {
            Mode::NoSession => perform_no_session_test().await,
            Mode::GlobalSession => perform_global_session_test().await,
            Mode::DBSession => perform_db_session_test().await,
            Mode::TableSession => perform_table_session_test().await,
            Mode::Seed => unreachable!(),
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
