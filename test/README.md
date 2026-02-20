# Testing this extension

This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements.

## Test Environment Setup

Testing this extension requires a running Hive Metastore instance with sample data. The test environment includes:
- Hive Metastore (with PostgreSQL backend)
- MinIO (S3-compatible storage)
- Spark seeder (populates sample test data)

## Running Tests

The root Makefile provides several targets for testing:

```bash
# Full integration test: builds, starts env, runs tests, cleans up
make test-all

# Manual control - start the test environment
make test-env-start

# Run tests only (environment must be running)
make test-run

# Stop the test environment
make test-env-stop

# Run tests without Docker (requires external HMS)
make test
# or
make test_debug
```

## Test Data

The `seed_hive_data_spark.py` script creates sample tables in various formats:
- **Parquet**: customers, reviews, type_test, decimal_partitioned, multi_partition
- **Delta Lake**: products
- **CSV**: orders
- **Iceberg**: inventory (if available)
- **Avro**: shipments (if available)

These tables test different storage formats, partitioning strategies, and type mappings to ensure the extension works correctly with the Hive ecosystem.