demo of parsing postgres replication stream with rust based materialize's [postgres utils](https://dev.materialize.com/api/rust/mz_postgres_util/struct.Config.html) and their [replication code](https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/postgres.rs#L507)

## Requirements

A postgres instance with a `testdb` dfatabase and a `testtbl` table with a `id` and `name` column.

```postgresql
CREATE DATABASE testdb;
\c testdb;
CREATE TABLE testtbl (id int PRIMARY KEY, name text);

CREATE PUBLICATION testpub FOR TABLE testtbl;
INSERT INTO testtbl VALUES (1, 'snot');
```

rust & cargo

## Usage

Run with the postgres connection string as the first argument.

```bash
cargo run -- "host=127.0.0.1 port=5433 user=postgres password=password dbname=testdb"
```

potentially the starting position can be provided by a 2nd parameter

```bash
cargo run -- "host=127.0.0.1 port=5433 user=postgres password=password dbname=testdb" 0/17773B0
```


## Notes
the tables set for replication needs to have a primary key otherwise you get an error about Replication identity missing for the table updates
```sql
ALTER TABLE testtbl REPLICA IDENTITY DEFAULT;
```