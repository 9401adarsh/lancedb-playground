mod seed;
mod utils;
use std::env;
use std::vec::Vec;
use lancedb::index::Index;

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
        println!("Global session test complete. Exiting.");
        return;
    }

    if mode == Mode::DBSession {
        println!("Database session test complete. Exiting.");
        return;
    }

    if mode == Mode::TableSession {
        println!("Table session test complete. Exiting.");
        return;
    }
}
