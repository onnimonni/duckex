# Library using DuckDB with Elixir

This uses DuckDB rust bindings to create a port for elixir to use.

It contains rust code is in `./native` folder which interacts with DuckDB rust library.

## Testing
Remember to run mix test and linters before you're finished


## Querying remote CSV files with duckdb
```sh
$ duckdb -c "
    INSTALL httpfs;
    LOAD httpfs;

    SELECT *
    FROM 'https://blobs.duckdb.org/nl-railway/services-2023.csv.gz'
"
```