-- Minimal canonical cross-engine scenarios for the HMS extension.
--
-- WHY: documents the smallest write surface (Parquet + CSV, both INSERT and CTAS)
-- that should be readable by a separate engine via the same Hive Metastore.
-- The full sqllogictest suite (test/sql/**) is the source of truth for CI; this
-- file is a hand-runnable reference for ad-hoc cross-engine debugging.
--
-- Invoked by `make test-cross-engine-scenarios-run` (which is itself called by
-- `make test-spark-verify` as Tier 4 of the cross-engine job in CI). Each xverify_*
-- table here is checked by test/spark_verify_writes.py SCENARIO_EXPECTED.
--
-- Run manually against an already-running env with:
--   make test-cross-engine-scenarios-run

CREATE OR REPLACE SECRET s3 (
    TYPE S3, PROVIDER config,
    KEY_ID 'minioadmin', SECRET 'minioadmin',
    ENDPOINT 'localhost:9000', REGION 'us-east-1',
    URL_STYLE 'path', USE_SSL false
);

ATTACH 'thrift://localhost:9083' AS hms (TYPE hive_metastore);

-- WHY: HMSCatalogSet::DropEntry currently throws NotImplementedException for the
-- generic schema-level DropEntry path, but DROP TABLE on a known table works
-- (see test/sql/parquet/ctas_orphan_metadata.test). We DROP first to keep this
-- script idempotent across runs without relying on CREATE OR REPLACE TABLE,
-- which the catalog has not been validated to support for all write paths.

-- Parquet INSERT scenario.
DROP TABLE IF EXISTS hms.sample_db.xverify_parquet_ins;
CREATE TABLE hms.sample_db.xverify_parquet_ins (id INTEGER, name VARCHAR)
WITH (format='parquet', location='s3a://test-bucket/xverify_parquet_ins/');
INSERT INTO hms.sample_db.xverify_parquet_ins VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Parquet CTAS scenario.
-- DuckDB 1.4 note: CTAS-with-WITH-clause is rejected at parse ("Unimplemented
-- features for CREATE TABLE as"). Use CREATE + INSERT-SELECT as equivalent.
DROP TABLE IF EXISTS hms.sample_db.xverify_parquet_ctas;
CREATE TABLE hms.sample_db.xverify_parquet_ctas (customer_id INTEGER, first_name VARCHAR)
WITH (format='parquet', location='s3a://test-bucket/xverify_parquet_ctas/');
INSERT INTO hms.sample_db.xverify_parquet_ctas
SELECT customer_id, first_name FROM hms.sample_db.customers WHERE customer_id <= 5;

-- CSV INSERT scenario.
DROP TABLE IF EXISTS hms.sample_db.xverify_csv_ins;
CREATE TABLE hms.sample_db.xverify_csv_ins (id INTEGER, label VARCHAR)
WITH (format='csv', location='s3a://test-bucket/xverify_csv_ins/');
INSERT INTO hms.sample_db.xverify_csv_ins VALUES (1, 'alpha'), (2, 'beta');

-- CSV CTAS scenario. See Parquet CTAS note above re: 1.4 CTAS-with-WITH.
DROP TABLE IF EXISTS hms.sample_db.xverify_csv_ctas;
CREATE TABLE hms.sample_db.xverify_csv_ctas (customer_id INTEGER, first_name VARCHAR)
WITH (format='csv', location='s3a://test-bucket/xverify_csv_ctas/');
INSERT INTO hms.sample_db.xverify_csv_ctas
SELECT customer_id, first_name FROM hms.sample_db.customers WHERE customer_id <= 4;

DETACH hms;
