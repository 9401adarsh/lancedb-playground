mod seed;
mod utils;
use lancedb::index::Index;
use std::env;
use std::time::Instant;
use std::vec::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Seed,
    GlobalSession,
    DBSession,
    TableSession,
}

fn parse_mode() -> Mode {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("Please provide a mode argument: seed, global, db, or table");
    }
    match args[1].as_str() {
        "seed" => Mode::Seed,
        "global_session" => Mode::GlobalSession,
        "db_session" => Mode::DBSession,
        "table_session" => Mode::TableSession,
        _ => {
            panic!("Invalid mode argument. Use: seed, global_session, db_session, or table_session")
        }
    }
}

async fn perform_global_session_test() {
    let uris: Vec<String> = vec!["./data/db1/".to_string(), "./data/db2/".to_string()];
    let table_names: Vec<String> = vec!["table_1".to_string(), "table_2".to_string()];

    println!("[global_session] Setting up one shared Session across all DBs...");
    let (session, connections) = utils::setup_global_session(&uris, None, None)
        .await
        .expect("failed to setup global session");
    println!("[global_session] Session: {:?}", session);

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

            println!(
                "[global_session] {}::{} => {} batches, {} rows in {:?}",
                uri,
                table_name,
                batches.len(),
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                elapsed
            );
            println!("[global_session] After Search: Session: {:?}", session);
            println!("[global_session] After Search: Session Stats:");
            utils::get_session_stats(&session).await;
        }
    }
}

async fn perform_db_session_test() {
    let uris: Vec<String> = vec!["./data/db1/".to_string(), "./data/db2/".to_string()];
    let table_names: Vec<String> = vec!["table_1".to_string(), "table_2".to_string()];

    println!("[db_session] Setting up one Session per DB...");
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

            println!(
                "[db_session] {}::{} => {} batches, {} rows in {:?}",
                uri,
                table_name,
                batches.len(),
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                elapsed
            );
            println!(
                "[db_session] After Search: Session for {}: {:?}",
                uri, _session
            );
            println!("[db_session] After Search: Session Stats:");
            utils::get_session_stats(&_session).await;
        }
    }
}

async fn perform_table_session_test() {
    // Placeholder for future implementation of table session test
    let uris: Vec<String> = vec!["./data/db1/".to_string(), "./data/db2/".to_string()];
    let table_names: Vec<String> = vec!["table_1".to_string(), "table_2".to_string()];

    println!("[table_session] Setting up one Session per (DB, table)...");
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

        println!(
            "[table_session] {} => {} batches, {} rows in {:?}",
            key,
            batches.len(),
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            elapsed
        );

        println!(
            "[table_session] After Search: Session for {}: {:?}",
            key, _session
        );
        println!("[table_session] After Search: Session Stats:");
        utils::get_session_stats(&_session).await;
    }
}

#[tokio::main]
async fn main() {
    let mode = parse_mode();
    println!("Running in mode: {:?}", mode);

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

    if mode == Mode::GlobalSession {
        perform_global_session_test().await;
        println!("Global session test complete. Exiting.");
        return;
    }

    if mode == Mode::DBSession {
        perform_db_session_test().await;
        println!("Database session test complete. Exiting.");
        return;
    }

    if mode == Mode::TableSession {
        perform_table_session_test().await;
        println!("Table session test complete. Exiting.");
        return;
    }
}
