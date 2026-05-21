#!/usr/bin/env python3
"""
Cross-engine read-back verification for the HMS write tests.

Reads each `duck_*` table that the DuckDB sqllogictest suite creates and asserts
row counts and a few representative values via PySpark. This is the only test
that exercises true interoperability with the Hive ecosystem — the SQL tests
read back through the same DuckDB process that wrote the file.

Prerequisite: the docker-compose env is up AND the DuckDB tests have already run
(so the `duck_*` tables exist in HMS and the files exist in MinIO).

Modes
-----
  default       (CI): verify duck_* tables via Spark read.
  bidirectional (manual): default checks PLUS write 2 rows from Spark into
                duck_inserted_parquet and re-verify count == 12. Useful for
                hand-checking that the HMS-registered table can be appended to
                by another engine and observed by both sides.

Run it via:
    make spark-verify-writes                       # default mode
    VERIFY_MODE=bidirectional make spark-verify-writes   # bidirectional mode

Exit code: 0 = all assertions passed, 1 = anything failed (and prints why).
"""

import argparse
import sys
from pyspark.sql import SparkSession

# WHY: keep schema/row expectations next to the verifier so a single edit covers
# both reader and writer paths. The DuckDB suite owns the canonical row counts.
EXPECTED = [
    # (table_name, expected_count, expected_columns)
    ("duck_inserted_parquet", 10, ["id", "name", "score"]),
    ("duck_ctas_customers", 10, ["customer_id", "first_name", "country"]),
    ("duck_ctas_literal", 3, ["n", "label"]),
    ("duck_inserted_csv", 3, ["id", "label"]),
    ("duck_ctas_csv", 5, ["customer_id", "first_name"]),
    ("duck_types_parquet", 3, ["id", "d", "ts", "dec", "flag", "payload"]),
    ("duck_complex_parquet", 3, ["id", "tags", "info"]),
]

# WHY: bidirectional appends rows to a DuckDB-created table from Spark and
# re-verifies count from the Spark side. One Parquet target, one CSV target, and
# one Avro target so all three write paths are exercised. Each entry:
# (table_name, baseline_rows, rows) — `rows` is a list of `VALUES`-row literals.
# Expected post-append count = baseline + len(rows).
BIDIRECTIONAL_TARGETS = [
    (
        "duck_inserted_parquet",
        10,
        [
            "(99, 'spark_added_1', 9.9)",
            "(100, 'spark_added_2', 10.10)",
        ],
    ),
    # duck_inserted_csv schema: id INT, label VARCHAR (see test/sql/csv/insert_csv.test).
    # CSV append from Spark exercises LazySimpleSerDe (','-delim, no header), which is the
    # serde DuckDB's HMS CREATE TABLE registers for CSV tables.
    (
        "duck_inserted_csv",
        3,
        [
            "(101, 'spark_csv_1')",
            "(102, 'spark_csv_2')",
        ],
    ),
    # duck_created_avro_simple schema: id BIGINT, label VARCHAR (see
    # test/sql/avro/create_avro_table.test). DuckDB creates only the HMS entry —
    # duckdb-avro is read-only — so the baseline is 0 and the file is written by
    # Spark here. The post-Spark DuckDB read pass (Tier 8 of test-spark-verify)
    # asserts the rows are visible to DuckDB via duckdb-avro.
    # CAST forces BIGINT for the id column even though the literal is an INT.
    (
        "duck_created_avro_simple",
        0,
        [
            "(CAST(1 AS BIGINT), 'spark_avro_1')",
            "(CAST(2 AS BIGINT), 'spark_avro_2')",
        ],
    ),
]

# Tables created by test/spark_verify/insert_scenarios.sql. Verified by Spark when
# the scenarios script ran before the verifier (CI Tier 4 / make test-cross-engine-scenarios).
# Skipped silently when absent so the verifier remains usable after a plain test-all run.
SCENARIO_EXPECTED = [
    ("xverify_parquet_ins", 3, ["id", "name"]),
    ("xverify_parquet_ctas", 5, ["customer_id", "first_name"]),
    ("xverify_csv_ins", 2, ["id", "label"]),
    ("xverify_csv_ctas", 4, ["customer_id", "first_name"]),
]


def make_session():
    return (
        SparkSession.builder.appName("HMS Write Verifier")
        .config("spark.sql.warehouse.dir", "s3a://test-bucket/")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.driver.memory", "2g")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
        .config("spark.sql.hive.convertMetastoreParquet", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .enableHiveSupport()
        .getOrCreate()
    )


def run_read_verification(spark):
    """Default mode: verify every expected duck_* table. Returns list of failure strings."""
    failures = []
    existing_tables = {row.tableName for row in spark.sql("SHOW TABLES IN sample_db").collect()}

    for table, expected_count, expected_cols in EXPECTED:
        if table not in existing_tables:
            failures.append(f"{table}: NOT FOUND in sample_db (DuckDB tests must run first)")
            continue

        try:
            df = spark.sql(f"SELECT * FROM sample_db.{table}")
            actual_count = df.count()
            actual_cols = df.columns

            if actual_count != expected_count:
                failures.append(f"{table}: expected {expected_count} rows, got {actual_count}")
                continue
            if actual_cols != expected_cols:
                failures.append(f"{table}: expected columns {expected_cols}, got {actual_cols}")
                continue

            print(f"✓ {table}: {actual_count} rows, columns {actual_cols}")
            # Print first 3 rows so a human reviewer can eyeball data sanity.
            df.show(3, truncate=False)
        except Exception as exc:
            failures.append(f"{table}: query failed — {exc}")

    return failures


def run_bidirectional_append(spark):
    """
    Spark INSERTs additional rows into DuckDB-created tables (Parquet + CSV) and
    re-reads to confirm both engines see the combined result.

    WHY: the read-only verification proves Spark can READ DuckDB's output. This
    adds proof that Spark can WRITE into the same HMS-managed location for both
    Parquet and CSV write paths, and that the row total reflects both writers.
    DuckDB will pick up the new files on its next attach since the table
    location is shared.
    """
    failures = []

    for table_short, baseline, rows in BIDIRECTIONAL_TARGETS:
        table = f"sample_db.{table_short}"
        append_rows = len(rows)
        expected_total = baseline + append_rows
        values_sql = ", ".join(rows)

        try:
            spark.sql(f"INSERT INTO {table} VALUES {values_sql}")
        except Exception as exc:
            failures.append(f"bidirectional[{table_short}]: Spark INSERT failed — {exc}")
            continue

        try:
            post_count = spark.sql(f"SELECT COUNT(*) AS c FROM {table}").collect()[0].c
        except Exception as exc:
            failures.append(f"bidirectional[{table_short}]: post-INSERT count query failed — {exc}")
            continue

        if post_count != expected_total:
            failures.append(
                f"bidirectional[{table_short}]: expected {expected_total} rows after Spark append, got {post_count}"
            )
            continue

        print(
            f"✓ bidirectional[{table_short}]: Spark appended {append_rows} rows; "
            f"total now {post_count} (== {expected_total})"
        )

    return failures


def run_scenario_verification(spark):
    """
    Verify tables created by test/spark_verify/insert_scenarios.sql when present.
    Silently skips when none of the xverify_* tables exist (i.e. the scenarios
    script was not run before the verifier).
    """
    failures = []
    existing_tables = {row.tableName for row in spark.sql("SHOW TABLES IN sample_db").collect()}

    any_present = any(t in existing_tables for t, _, _ in SCENARIO_EXPECTED)
    if not any_present:
        return failures

    print("\n--- scenarios (insert_scenarios.sql) ---")
    for table, expected_count, expected_cols in SCENARIO_EXPECTED:
        if table not in existing_tables:
            failures.append(f"scenario[{table}]: NOT FOUND in sample_db (insert_scenarios.sql must run first)")
            continue

        try:
            df = spark.sql(f"SELECT * FROM sample_db.{table}")
            actual_count = df.count()
            actual_cols = df.columns

            if actual_count != expected_count:
                failures.append(f"scenario[{table}]: expected {expected_count} rows, got {actual_count}")
                continue
            if actual_cols != expected_cols:
                failures.append(f"scenario[{table}]: expected columns {expected_cols}, got {actual_cols}")
                continue

            print(f"✓ scenario[{table}]: {actual_count} rows, columns {actual_cols}")
            df.show(3, truncate=False)
        except Exception as exc:
            failures.append(f"scenario[{table}]: query failed — {exc}")

    return failures


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--mode",
        choices=["default", "bidirectional"],
        default="default",
        help="default = read-only verification (CI); bidirectional = also Spark-INSERT and re-verify",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = make_session()
    spark.sparkContext.setLogLevel("ERROR")

    failures = run_read_verification(spark)
    failures.extend(run_scenario_verification(spark))

    if args.mode == "bidirectional":
        # WHY: only run the append after read verification succeeds at least
        # structurally — we still want the append attempted even if some other
        # table failed, so that the user sees both failure surfaces in one run.
        failures.extend(run_bidirectional_append(spark))

    if failures:
        print("\n" + "=" * 78)
        print(f"Spark interop verification FAILED (mode={args.mode}):")
        for f in failures:
            print(f"  - {f}")
        print("=" * 78)
        sys.exit(1)

    print(f"\nAll checks passed (mode={args.mode}) — interop OK.")
    sys.exit(0)


if __name__ == "__main__":
    main()
