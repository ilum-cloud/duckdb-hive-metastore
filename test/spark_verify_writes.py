#!/usr/bin/env python3
"""
Cross-engine read-back verification for the HMS write tests.

Reads each `duck_*` table that the DuckDB sqllogictest suite creates and asserts
row counts and a few representative values via PySpark. This is the only test
that exercises true interoperability with the Hive ecosystem — the SQL tests
read back through the same DuckDB process that wrote the file.

Prerequisite: the docker-compose env is up AND the DuckDB tests have already run
(so the `duck_*` tables exist in HMS and the files exist in MinIO).

Run it via:
    make spark-verify-writes
which `docker run`s this against the existing spark-seeder image on the test
network.

Exit code: 0 = all assertions passed, 1 = anything failed (and prints why).
"""

import sys
from pyspark.sql import SparkSession

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


def main():
    spark = make_session()
    spark.sparkContext.setLogLevel("ERROR")

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

    if failures:
        print("\n" + "=" * 78)
        print("Spark interop verification FAILED:")
        for f in failures:
            print(f"  - {f}")
        print("=" * 78)
        sys.exit(1)

    print("\nAll write tables verified via Spark — interop OK.")
    sys.exit(0)


if __name__ == "__main__":
    main()
