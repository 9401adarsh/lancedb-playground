use crate::log_println;
use crate::seed;
use arrow_array::RecordBatch;
use futures_util::TryStreamExt;
use lancedb::index::Index;
use lancedb::query::ExecutableQuery;
use rand::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub type DbConnectionPool = HashMap<String, lancedb::Connection>;
pub type DbSessionPool = HashMap<String, (Arc<lancedb::Session>, lancedb::Connection)>;
pub type TableSessionPool =
    HashMap<String, (Arc<lancedb::Session>, lancedb::Connection, lancedb::Table)>;

pub fn make_session(
    index_cache_limit: Option<usize>,
    metadata_cache_limit: Option<usize>,
) -> Arc<lancedb::Session> {
    // Cache budgets (bytes) — tune later
    let index_cache_bytes: usize = index_cache_limit.unwrap_or(8 * 1024 * 1024 * 1024); // 8 GB
    let metadata_cache_bytes: usize = metadata_cache_limit.unwrap_or(512 * 1024 * 1024); // 512 MB
    let store_registry = Arc::new(lancedb::ObjectStoreRegistry::default());
    Arc::new(lancedb::Session::new(
        index_cache_bytes,
        metadata_cache_bytes,
        store_registry,
    ))
}

pub async fn connect_to_db(
    session: Arc<lancedb::Session>,
    uri: &str,
) -> Result<lancedb::Connection, lancedb::Error> {
    // construct the URI for the database and connect with the provided session and execute the connection
    let db_connect_handle = lancedb::connect(uri).session(session).execute().await;
    db_connect_handle
}

pub async fn connect_to_db_no_session(
    uri: &str,
) -> Result<lancedb::Connection, lancedb::Error> {
    // Connect without an explicit session — the library creates a default one internally
    lancedb::connect(uri).execute().await
}

pub async fn setup_no_session(
    uris: &[String],
) -> Result<DbConnectionPool, lancedb::Error> {
    let mut pool = create_connection_pool();
    for uri in uris.iter() {
        match connect_to_db_no_session(uri).await {
            Ok(connection) => {
                println!("[no_session] Connected to {} (default session)", uri);
                pool.insert(uri.to_string(), connection);
            }
            Err(e) => eprintln!("[no_session] Failed to connect to {}: {}", uri, e),
        }
    }
    Ok(pool)
}

pub fn create_connection_pool() -> DbConnectionPool {
    HashMap::new()
}

pub async fn add_connection_to_pool(
    pool: &mut DbConnectionPool,
    session: Arc<lancedb::Session>,
    uri: &str,
) -> Result<(), lancedb::Error> {
    match connect_to_db(session, uri).await {
        Ok(connection) => {
            println!("Successfully connected to database at URI: {}", uri);
            pool.insert(uri.to_string(), connection);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

pub async fn setup_global_session(
    uris: &[String],
    index_cache_limit: Option<usize>,
    metadata_cache_limit: Option<usize>,
) -> Result<(Arc<lancedb::Session>, DbConnectionPool), lancedb::Error> {
    let session = make_session(index_cache_limit, metadata_cache_limit);
    let mut pool = create_connection_pool();
    for uri in uris.iter() {
        match add_connection_to_pool(&mut pool, session.clone(), uri).await {
            Ok(_) => println!("Connection added to pool for URI: {}", uri),
            Err(e) => eprintln!("Failed to connect to {}: Error: {}", uri, e),
        }
    }
    println!("Global session created: {:?}", session);
    Ok((session, pool))
}

pub async fn setup_db_session(
    uris: &[String],
    index_cache_limit: Option<usize>,
    metadata_cache_limit: Option<usize>,
) -> Result<DbSessionPool, lancedb::Error> {
    let mut pool: DbSessionPool = HashMap::new();
    for uri in uris.iter() {
        let session = make_session(index_cache_limit, metadata_cache_limit);
        let connection = connect_to_db(session.clone(), uri).await?;
        pool.insert(uri.to_string(), (session, connection));
    }
    println!(
        "Database session created with connections: {:?}",
        pool.keys()
    );
    Ok(pool)
}

pub async fn setup_table_session(
    uri: &str,
    table_name: &str,
    index_cache_limit: Option<usize>,
    metadata_cache_limit: Option<usize>,
) -> Result<(Arc<lancedb::Session>, lancedb::Connection, lancedb::Table), lancedb::Error> {
    let session = make_session(index_cache_limit, metadata_cache_limit);
    println!("Session created for table session: {:?}", session);
    let connection = connect_to_db(session.clone(), uri).await?;
    println!("Connected to database at URI: {}", uri);
    let num_rows = 0;
    let table = open_or_create_seeded_table(&connection, table_name, num_rows).await?;
    println!(
        "Table '{}' is ready in database at URI: {}",
        table_name, uri
    );
    Ok((session, connection, table))
}

pub async fn add_table_session_to_pool(
    pool: &mut TableSessionPool,
    uri: &str,
    table_name: &str,
    index_cache_limit: Option<usize>,
    metadata_cache_limit: Option<usize>,
) -> Result<(), lancedb::Error> {
    match setup_table_session(uri, table_name, index_cache_limit, metadata_cache_limit).await {
        Ok((session, connection, table)) => {
            let key = format!("{}::{}", uri, table_name);
            pool.insert(key.clone(), (session, connection, table));
            println!("Table session added to pool for key: {}", key);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

pub async fn create_table(
    connection: &lancedb::Connection,
    table_name: &str,
    num_rows: usize,
) -> Result<lancedb::Table, lancedb::Error> {
    let reader = seed::make_seed_batches(num_rows).expect("Failed to create seed batches");
    let table = connection
        .create_table(table_name, reader)
        .execute()
        .await?;
    Ok(table)
}

pub async fn open_or_create_seeded_table(
    connection: &lancedb::Connection,
    table_name: &str,
    num_rows: usize,
) -> Result<lancedb::Table, lancedb::Error> {
    match connection.open_table(table_name).execute().await {
        Ok(table) => {
            println!(
                "Table '{}' already exists. Opened existing table.",
                table_name
            );
            Ok(table)
        }
        Err(e) => {
            println!(
                "Table '{}' does not exist. Attempting to create it. Error: {}",
                table_name, e
            );
            create_table(connection, table_name, num_rows).await
        }
    }
}

pub async fn ensure_vector_index(
    table: &lancedb::Table,
    table_name: &str,
    column: &str,
    index: Index,
) -> Result<(), lancedb::Error> {
    let columns = vec![column.to_string()];
    let result = table.create_index(&columns, index.clone()).execute().await;
    match result {
        Ok(_) => {
            println!(
                "Index of type {:?} already exists or created successfully on columns {:?} for table '{}'",
                index, columns, table_name
            );
            Ok(())
        }
        Err(e) => {
            eprintln!(
                "Failed to create index of type {:?} on columns {:?} for table '{}': Error: {}",
                index, columns, table_name, e
            );
            Err(e)
        }
    }
}

pub async fn run_nearest_to_vector_search_k(
    table: &lancedb::Table,
    query_vector: Vec<f32>,
    _column: &str,
    k: usize,
) -> Result<Vec<RecordBatch>, lancedb::Error> {
    let stream = table
        .query()
        .nearest_to(query_vector)
        .unwrap()
        .nprobes(k)
        .execute()
        .await
        .unwrap();

    let result: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    Ok(result)
}

pub fn random_query_vec(dim: usize) -> Vec<f32> {
    let mut rng = rand::rng();
    (0..dim)
        .map(|_| rng.random_range(0.0..1.0) as f32)
        .collect()
}

pub async fn get_session_stats(session: &Arc<lancedb::Session>) {
    log_println!("Index cache stats: {:#?}", session.index_cache_stats().await);
    log_println!("Metadata cache stats: {:#?}", session.metadata_cache_stats().await);
}
