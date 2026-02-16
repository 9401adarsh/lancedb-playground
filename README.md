## Session Cache Benchmarking for Lance DB Rust SDK

This repository is a first attempt at creating a LanceDB session cache benchmark suite. We try to study the effects b/w allocating a session cache in a global fashion (shared across DBs), a session cache per DB, and a session cache for each table in a DB. 

The following bench suite has been implemented in Rust. 

Please ensure you have `cargo` setup for running this project.

## Running the test suite

- First create a `data/` directory under project root. Under `data/` create `db1/` and `db2/` respectively. 
  
- Run the project to seed data into the DBs under `db1/` and `db2/` /using `NUM_SEED_ROWS=<num of rows you want to seed> cargo run seed`.

- Following this run the project in various modes `cargo run global_session` (to observe session cache stats when one session is shared amongst multiple dbs), `cargo run db_session` (to observe session cache behaviour when a session is generated for every db), and `cargo run table_session` (to observe session cache behavior when a session is created for every table). 