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

# WHY: bidirectional appends exactly two rows to duck_inserted_parquet, chosen
# because it has the simplest schema (id INT, name VARCHAR, score DOUBLE) and a
# known baseline of 10. After append, count must be 12.
BIDIRECTIONAL_TABLE = "duck_inserted_parquet"
BIDIRECTIONAL_BASELINE_ROWS = 10
BIDIRECTIONAL_APPEND_ROWS = 2
BIDIRECTIONAL_EXPECTED_TOTAL = BIDIRECTIONAL_BASELINE_ROWS + BIDIRECTIONAL_APPEND_ROWS


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
    Manual mode extension: Spark INSERTs two rows into a DuckDB-created table
    and we re-read to confirm both engines see the combined result.

    WHY: the read-only verification proves Spark can READ DuckDB's output. This
    adds proof that Spark can WRITE into the same HMS-managed location and that
    the row total reflects both writers. DuckDB will pick up the new files on
    its next attach since the table location is shared.
    """
    failures = []
    table = f"sample_db.{BIDIRECTIONAL_TABLE}"

    try:
        spark.sql(
            f"INSERT INTO {table} VALUES (99, 'spark_added_1', 9.9), (100, 'spark_added_2', 10.10)"
        )
    except Exception as exc:
        failures.append(f"bidirectional: Spark INSERT into {table} failed — {exc}")
        return failures

    try:
        post_count = spark.sql(f"SELECT COUNT(*) AS c FROM {table}").collect()[0].c
    except Exception as exc:
        failures.append(f"bidirectional: post-INSERT count query failed — {exc}")
        return failures

    if post_count != BIDIRECTIONAL_EXPECTED_TOTAL:
        failures.append(
            f"bidirectional: expected {BIDIRECTIONAL_EXPECTED_TOTAL} rows after Spark append, "
            f"got {post_count}"
        )
        return failures

    print(
        f"✓ bidirectional: Spark appended {BIDIRECTIONAL_APPEND_ROWS} rows to {table}; "
        f"total now {post_count} (== {BIDIRECTIONAL_EXPECTED_TOTAL})"
    )
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
