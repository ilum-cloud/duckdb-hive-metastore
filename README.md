# Hive Metastore Extension for DuckDB

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

**DuckDB Hive Metastore (HMS) extension** enables DuckDB to connect to Apache Hive Metastore via Thrift protocol and query tables stored in data lakes (S3, MinIO, Azure, GCS, HDFS, etc.). The extension provides seamless integration with the Hive ecosystem while leveraging DuckDB's powerful analytical capabilities.

## Features

**Supported Table Formats:**

- **Hive Tables:** Parquet, CSV/Text, ORC (partial support)
- **Modern Table Formats:** Delta Lake, Apache Iceberg
- **Avro Tables:** Full support with automatic type mapping(\*)

**Key Capabilities:**

- Direct Thrift communication with Hive Metastore (no REST API dependencies)
- Automatic format detection from HMS metadata
- Hive-style partitioning support
- S3-compatible object storage integration (MinIO, AWS S3, etc.)
- Schema and database discovery
- Spark-created table compatibility (including complex types)
- Automatic type mapping for format-specific limitations (e.g., Avro date handling)

## SQL Feature Support Matrix

The following table shows which SQL operations are supported for each table format in the Hive Metastore extension:

| Feature             | Parquet | Delta Lake              | Iceberg                 | CSV/Text             | Avro | ORC |
| ------------------- | ------- | ----------------------- | ----------------------- | -------------------- | ---- | --- |
| **SELECT** (Read)   | ✅      | ✅                      | ✅¹                     | ✅                   | ✅²  | ⚠️³ |
| **INSERT**          | ❌      | ⛔ Extension Limitation | ❌                      | ❌                   | ❌   | ❌  |
| **UPDATE**          | ❌      | ⛔ Extension Limitation | ⛔ Extension Limitation | ❌                   | ❌   | ❌  |
| **DELETE**          | ❌      | ⛔ Extension Limitation | ⛔ Extension Limitation | ❌                   | ❌   | ❌  |
| **CREATE TABLE AS** | ❌      | ❌                      | ❌                      | ❌                   | ❌   | ❌  |
| **CREATE TABLE**    | ✅      | ⛔ Extension Limitation | ❌                      | ✅                   | ❌   | ⚠️³ |
| **COPY TO**         | ❌      | ❌                      | ❌                      | ❌                   | ❌   | ❌  |
| **Partitioning**    | ✅      | ✅                      | ✅                      | ✅                   | ✅   | ⚠️³ |
| **Complex Types**   | ✅      | ✅                      | ✅                      | ⛔ Format Limitation | ✅   | ⚠️³ |

### Legend

- ✅ **Supported**: Feature works as expected
- ⚠️ **Partial**: Feature works with limitations (see caveats below)
- ❌ **Not Supported**: Feature not implemented in HMS extension
- ⛔ **Extension / Format Limitation**: Feature not supported by underlying DuckDB extension or format

### Caveats and Notes

**¹ Iceberg Tables:**

- Require `unsafe_enable_version_guessing = true` for Spark-created tables (missing `version-hint.text` files)
- Despite the "unsafe" name, this is safe for read-only queries ([DuckDB docs](https://duckdb.org/docs/stable/core_extensions/iceberg/overview#guessing-metadata-versions))
- Only unsafe in concurrent write scenarios

**² Avro Tables:**

- Automatic type mapping for DATE → INTEGER and TIMESTAMP → BIGINT
- See [Working with Avro Tables](#working-with-avro-tables) for details and examples

**³ ORC Format:**

- Only partial support in DuckDB
- Some complex type combinations may not work correctly
- Reading support varies depending on ORC file encoding and compression

### Write Operation Requirements

For INSERT operations on Delta Lake tables:

```sql
-- Install and load the Delta extension
INSTALL delta;
LOAD delta;

-- Configure S3 credentials if needed
CREATE SECRET s3 (TYPE S3, ...);

-- INSERT works seamlessly
INSERT INTO hms.my_db.delta_table VALUES (1, 'value');
```

## Usage

### Quick Start

First, load the extension and any required format extensions:

```sql
-- Load the Hive Metastore extension
-- (not yet available in community repository, install from custom location)
INSTALL hive_metastore FROM '<your-extension-repository>';
LOAD hive_metastore;
```

### Connecting to Hive Metastore

Attach to a Hive Metastore using a Thrift connection string:

```sql
ATTACH 'thrift://<host>:<port>' AS <catalog_name> (<options>);
```

**Parameters:**

- `<host>:<port>`: HMS Thrift endpoint (default port is `9083`)
- `<catalog_name>`: Alias for the HMS catalog in DuckDB
- `<options>`: Additional options for the HMS connection:
  - `TYPE` (required): Must be set to `hive_metastore` to indicate that we want to use the Hive Metastore extension
  - `WAREHOUSE_LOCATION`: The warehouse location path. Used for table storage location resolution (mostly not required, but can be useful in some cases).
  - `DEFAULT_SCHEMA`: The database/schema name to use when queries don't specify one. Defaults to `default` if not provided.

**Example:**

```sql
ATTACH 'thrift://localhost:9083' AS my_hms (TYPE hive_metastore);
```

### Querying Tables

Once attached, query HMS tables like regular DuckDB tables:

```sql
-- List databases
SHOW DATABASES FROM my_hms;

-- List tables in a database
SHOW TABLES FROM my_hms.my_database;

-- Query a table
SELECT * FROM my_hms.my_database.my_table;

-- Use filters and aggregations
SELECT category, COUNT(*)
FROM my_hms.sales_db.transactions
WHERE date >= '2024-01-01'
GROUP BY category;
```

### Working with S3-backed Tables

For tables stored in S3-compatible storage, configure S3 credentials using DuckDB Secrets:

```sql
-- Create S3 secret
CREATE SECRET my_s3 (
    TYPE S3,
    KEY_ID 'your-access-key',
    SECRET 'your-secret-key',
    ENDPOINT 'optional-custom-endpoint',
    REGION 'us-east-1'
);

-- Now query S3-backed tables
SELECT * FROM my_hms.lakehouse.customers;
```

### Working with Iceberg Tables

Apache Iceberg tables require the `iceberg` extension and special configuration:

```sql
LOAD hive_metastore;
SET unsafe_enable_version_guessing = true;

-- Attach HMS and query Iceberg tables
ATTACH 'thrift://localhost:9083' AS hms (TYPE hive_metastore);

SELECT * FROM hms.lakehouse.iceberg_table;
```

> **Note:** Spark-created Iceberg tables require `unsafe_enable_version_guessing = true` because they don't include `version-hint.text` files. See the [SQL Feature Support Matrix](#sql-feature-support-matrix) for details.

### Working with Avro Tables

Avro tables are fully supported with automatic type mapping:

```sql
-- Install and load required extensions
INSTALL avro;
LOAD avro;
LOAD hive_metastore;

-- Attach HMS and query Avro tables
ATTACH 'thrift://localhost:9083' AS hms (TYPE hive_metastore);

SELECT * FROM hms.warehouse.shipments;
```

**Automatic Type Mapping:**

Due to Avro's limited type support ([see issue](https://github.com/duckdb/duckdb-avro/issues/6)), the HMS extension automatically converts:

- `DATE` → `INTEGER` (days since Unix epoch 1970-01-01)
- `TIMESTAMP` → `BIGINT` (microseconds since Unix epoch)
- Complex types (STRUCT, LIST, MAP) are recursively mapped

When filtering on date columns, use integer representation:

```sql
-- Filter by date (19723 = 2024-01-01)
SELECT * FROM hms.warehouse.shipments WHERE ship_date >= 19723;

-- Convert back to date if needed
SELECT shipment_id, ship_date::DATE + INTERVAL '0' DAY AS actual_date
FROM hms.warehouse.shipments;
```

### CREATE TABLE Support

The HMS extension now supports creating tables in the Hive Metastore from DuckDB using a standard SQL
CREATE TABLE statement. This is intended for basic managed/external table creation and focuses on
compatibility with common readers such as Spark and Trino.

- **Default format:** If no format is provided via tags, Parquet will be used.
- **Location behavior:** You must either provide a `location` tag in the `CREATE TABLE` statement, or
  attach the HMS catalog with the `WAREHOUSE_LOCATION` option (see example). When a `WAREHOUSE_LOCATION`
  is provided and `LOCATION` is omitted, a managed table directory `<warehouse>/<db>/<table>` will be used.
- **Table type:** The extension sets the Hive `tableType` appropriately (`MANAGED_TABLE` when managed,
  `EXTERNAL_TABLE` when an explicit external `LOCATION` is used).
- **Type mapping:** Basic DuckDB types are mapped to Hive/Thrift type strings. Complex types are supported
  when they can be represented as Hive types; Avro-specific mappings are applied for Avro tables.
- **Limitations:** This feature focuses on simple table creation (Parquet, CSV). Advanced table properties,
  custom SerDes, and provider-specific storage formats (Delta/Iceberg managed creation) are not supported
  as managed table creators by this extension.

Example — attach with a warehouse then create a table:

```sql
-- Attach HMS with a default warehouse location used for managed tables
-- In general use s3a:// for Hadoop-compatible S3 access
ATTACH 'thrift://localhost:9083' AS hms (TYPE hive_metastore, WAREHOUSE_LOCATION 's3a://my-bucket/warehouse');

-- Create the table (no explicit LOCATION required)
CREATE TABLE hms.default.nice (weight INT, height INT);

-- Create an external table with explicit location
CREATE TABLE hms.default.external_nice (a INT, b INT) WITH (location='s3a://my-bucket/other/path', format='parquet');
```

Error handling:

- Creating a table without `LOCATION` and without attaching the HMS catalog with a `WAREHOUSE_LOCATION`
  will fail and instruct the user to either specify `LOCATION` or attach with `WAREHOUSE_LOCATION`.

````

### Important Notes

- **Format Extensions:** The HMS extension delegates data reading to DuckDB's format scanners. Install required extensions (`delta`, `iceberg`, etc.) before querying those table types.
- **Partitioning:** Hive-style partitioning is automatically detected and applied during query execution.
- **Spark Compatibility:** Tables created by Spark (including complex types like structs, arrays, maps) are fully supported through Spark schema metadata parsing.

## Building

### Managing dependencies

This extension uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:

```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
````

However, it uses only the `thrift` package from VCPKG. You can also install it (`libthrift-dev` and `thrift-compiler` plus different dependencies of DuckDB itself) manually and skip VCPKG entirely if you prefer that.

### Build steps

Now to build the extension, run:

```sh
make
```

The main binaries that will be built are:

```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/hive_metastore/hive_metastore.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `hive_metastore.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension

To test out the extension, you can run the `duckdb` binary built in the previous step.

## Running the tests

As testing this extension requires a running Hive Metastore, the tests require some additional setup.

The simplest way to run the full integration test suite is:

```bash
make test-all
```

This command will:

1. Build the extension in release mode
2. Start the Docker Compose environment (Hive Metastore, PostgreSQL, MinIO)
3. Wait for the data seeding to complete (via PySpark)
4. Run all tests
5. Tear down the test environment

For manual control over the test environment, you can use these separate targets:

```bash
# Start the test environment manually
make test-env-start

# Run tests (assumes environment is already running)
make test-run

# Stop the test environment
make test-env-stop
```

**Note:** The test environment uses Docker Compose to spin up:

- Hive Metastore (with PostgreSQL backend)
- MinIO (S3-compatible storage)
- Spark seeder (populates sample data in various formats: Parquet, Delta, CSV, Iceberg, Avro)

The seeder script (`test/seed_hive_data_spark.py`) creates test tables with different storage formats and partitioning schemes to verify the extension's compatibility with the Hive ecosystem.

### Installing the deployed binaries

To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:

```shell
duckdb -unsigned
```

Python:

```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:

```js
db = new duckdb.Database(":memory:", { allow_unsigned_extensions: "true" });
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:

```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```

Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:

```sql
INSTALL hive_metastore;
LOAD hive_metastore;
```

## Extension updating

When cloning this template, the target version of DuckDB should be the latest stable release of DuckDB. However, there
will inevitably come a time when a new DuckDB is released and the extension repository needs updating. This process goes
as follows:

- Bump submodules
  - `./duckdb` should be set to latest tagged release
  - `./extension-ci-tools` should be set to updated branch corresponding to latest DuckDB release. So if you're building for DuckDB `v1.1.0` there will be a branch in `extension-ci-tools` named `v1.1.0` to which you should check out.
- Bump versions in `./github/workflows`
  - `duckdb_version` input in `duckdb-stable-build` job in `MainDistributionPipeline.yml` should be set to latest tagged release
  - `duckdb_version` input in `duckdb-stable-deploy` job in `MainDistributionPipeline.yml` should be set to latest tagged release
  - the reusable workflow `duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml` for the `duckdb-stable-build` job should be set to latest tagged release

### API changes

DuckDB extensions built with this extension template are built against the internal C++ API of DuckDB. This API is not guaranteed to be stable.
What this means for extension development is that when updating your extensions DuckDB target version using the above steps, you may run into the fact that your extension no longer builds properly.

Currently, DuckDB does not (yet) provide a specific change log for these API changes, but it is generally not too hard to figure out what has changed.

For figuring out how and why the C++ API changed, we recommend using the following resources:

- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ Header file of the API that has changed

## Setting up CLion

### Opening project

Configuring CLion with this extension requires a little work. Firstly, make sure that the DuckDB submodule is available.
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

### Debugging

To set up debugging in CLion, there are two simple steps required. Firstly, in `CLion -> Settings / Preferences -> Build, Execution, Deploy -> CMake` you will need to add the desired builds (e.g. Debug, Release, RelDebug, etc). There's different ways to configure this, but the easiest is to leave all empty, except the `build path`, which needs to be set to `../build/{build type}`, and CMake Options to which the following flag should be added, with the path to the extension CMakeList:

```
-DDUCKDB_EXTENSION_CONFIGS=<path_to_the_extension_CMakeLists.txt>
```

The second step is to configure the unittest runner as a run/debug configuration. To do this, go to `Run -> Edit Configurations` and click `+ -> Cmake Application`. The target and executable should be `unittest`. This will run all the DuckDB tests. To specify only running the extension specific tests, add `--test-dir ../../.. [sql]` to the `Program Arguments`. Note that it is recommended to use the `unittest` executable for testing/development within CLion. The actual DuckDB CLI currently does not reliably work as a run target in CLion.

## Support

This extension is currently in active development. Please report any issues on the GitHub repository.
