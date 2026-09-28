#!/usr/bin/env bash
set -euo pipefail

# Fixture for the partition tests in test/sql/hive/.
#
# Creates two partitioned Parquet tables that Spark cannot produce:
#   * duck_fixture_nonstd  - partition directories do NOT follow Hive's key=value convention, one
#                            partition is stored outside the table location, one partition holds the
#                            NULL marker value (__HIVE_DEFAULT_PARTITION__), and one holds a data
#                            file named the way Hive names them, without an extension. Its partitions
#                            carry the zero statistics Hive records before any file is written.
#   * duck_fixture_noparts - declares partition columns but has no partition registered at all.
#   * duck_fixture_schema_mix - partition locations sort in the opposite order to the partitions, so
#                            the first file under the table location belongs to the last partition.
#                            The tests give its files different column orders and column sets.
#   * duck_fixture_hms_types - every partition lives outside the table location, so the column types
#                            come from the metastore rather than from a data file. Its partitions carry
#                            a row count, as Hive records after gathering statistics.
#   * duck_fixture_nested  - one partition's location lies inside another's; the tests also put a
#                            directory under the table location that no partition points at.
#   * duck_fixture_values_in_files - declares a partition column but has no partition registered;
#                            the tests write files that hold the partition values themselves.
#
# The data files are written by the tests themselves with COPY: the metastore container runs no
# execution engine, so Hive can only do DDL here.
#
# Idempotent (IF NOT EXISTS, and the NULL-marker update is a no-op once applied). Must be run after
# `make test-env-start`. The Hive CLI needs writable scratch directories inside the container.

cd "$(dirname "$0")/../.."

docker compose exec -T hive-metastore hive \
  --hiveconf hive.exec.scratchdir=/tmp/hive_fixture_scratch \
  --hiveconf hive.exec.local.scratchdir=/tmp/hive_fixture_local_scratch \
  --hiveconf hive.downloaded.resources.dir=/tmp/hive_fixture_resources \
  -e "
CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.duck_fixture_nonstd (review_id INT, product_id INT)
  PARTITIONED BY (region STRING, year INT)
  STORED AS PARQUET
  LOCATION 's3a://test-bucket/duck_fixture_nonstd';

ALTER TABLE sample_db.duck_fixture_nonstd ADD IF NOT EXISTS
  PARTITION (region='production', year=2024) LOCATION 's3a://test-bucket/duck_fixture_nonstd/production/2024'
  PARTITION (region='production', year=2025) LOCATION 's3a://test-bucket/duck_fixture_nonstd/production/2025'
  PARTITION (region='staging', year=2024)    LOCATION 's3a://test-bucket/duck_fixture_outside/staging_2024'
  PARTITION (region='unknown', year=2025)    LOCATION 's3a://test-bucket/duck_fixture_nonstd/unknown/2025'
  PARTITION (region='hivefile', year=2026)   LOCATION 's3a://test-bucket/duck_fixture_nonstd/hivefile/2026';

CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.duck_fixture_noparts (review_id INT)
  PARTITIONED BY (region STRING)
  STORED AS PARQUET
  LOCATION 's3a://test-bucket/duck_fixture_noparts';

CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.duck_fixture_schema_mix (id INT, amount INT)
  PARTITIONED BY (batch STRING)
  STORED AS PARQUET
  LOCATION 's3a://test-bucket/duck_fixture_schema_mix';

ALTER TABLE sample_db.duck_fixture_schema_mix ADD IF NOT EXISTS
  PARTITION (batch='a') LOCATION 's3a://test-bucket/duck_fixture_schema_mix/z_dir'
  PARTITION (batch='b') LOCATION 's3a://test-bucket/duck_fixture_schema_mix/y_dir'
  PARTITION (batch='c') LOCATION 's3a://test-bucket/duck_fixture_schema_mix/a_dir';

CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.duck_fixture_hms_types (id BIGINT, label STRING)
  PARTITIONED BY (batch STRING)
  STORED AS PARQUET
  LOCATION 's3a://test-bucket/duck_fixture_hms_types';

ALTER TABLE sample_db.duck_fixture_hms_types ADD IF NOT EXISTS
  PARTITION (batch='a') LOCATION 's3a://test-bucket/duck_fixture_hms_types_data/a'
  PARTITION (batch='b') LOCATION 's3a://test-bucket/duck_fixture_hms_types_data/b';

CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.duck_fixture_nested (id INT)
  PARTITIONED BY (part STRING)
  STORED AS PARQUET
  LOCATION 's3a://test-bucket/duck_fixture_nested';

ALTER TABLE sample_db.duck_fixture_nested ADD IF NOT EXISTS
  PARTITION (part='outer') LOCATION 's3a://test-bucket/duck_fixture_nested/outer'
  PARTITION (part='inner') LOCATION 's3a://test-bucket/duck_fixture_nested/outer/inner';

CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.duck_fixture_values_in_files (id INT)
  PARTITIONED BY (region STRING)
  STORED AS PARQUET
  LOCATION 's3a://test-bucket/duck_fixture_values_in_files';
"

# Hive refuses to register __HIVE_DEFAULT_PARTITION__ through DDL ("reserved substring"), yet it
# writes exactly that value itself for rows whose partition column is NULL. Set it directly on the
# 'unknown' partition so the tests cover the NULL marker.
docker compose exec -T hive-metastore-postgresql \
  psql -U hive -d metastore -v ON_ERROR_STOP=1 <<'SQL'
UPDATE "PARTITION_KEY_VALS" v
   SET "PART_KEY_VAL" = '__HIVE_DEFAULT_PARTITION__'
  FROM "PARTITIONS" p
  JOIN "TBLS" t ON t."TBL_ID" = p."TBL_ID"
  JOIN "DBS"  d ON d."DB_ID"  = t."DB_ID"
 WHERE v."PART_ID" = p."PART_ID"
   AND d."NAME" = 'sample_db'
   AND t."TBL_NAME" = 'duck_fixture_nonstd'
   AND v."INTEGER_IDX" = 0
   AND v."PART_KEY_VAL" = 'unknown';

-- Row counts as Hive records them after gathering statistics, so the planner's estimate can come from the
-- metastore. The files the tests write hold far fewer rows, which is what tells the two sources apart.
INSERT INTO "PARTITION_PARAMS" ("PART_ID", "PARAM_KEY", "PARAM_VALUE")
SELECT p."PART_ID", 'numRows', '1000000'
  FROM "PARTITIONS" p
  JOIN "TBLS" t ON t."TBL_ID" = p."TBL_ID"
  JOIN "DBS"  d ON d."DB_ID"  = t."DB_ID"
 WHERE d."NAME" = 'sample_db'
   AND t."TBL_NAME" = 'duck_fixture_hms_types'
ON CONFLICT ("PART_ID", "PARAM_KEY") DO UPDATE SET "PARAM_VALUE" = EXCLUDED."PARAM_VALUE";

-- The statistics Hive records for a partition registered before its files exist, and keeps after other writers
-- add them: zero rows, bytes and files. They are stale, so the tests check they are not taken at face value.
INSERT INTO "PARTITION_PARAMS" ("PART_ID", "PARAM_KEY", "PARAM_VALUE")
SELECT p."PART_ID", k.key, '0'
  FROM "PARTITIONS" p
  JOIN "TBLS" t ON t."TBL_ID" = p."TBL_ID"
  JOIN "DBS"  d ON d."DB_ID"  = t."DB_ID"
 CROSS JOIN (VALUES ('numRows'), ('totalSize'), ('numFiles')) AS k(key)
 WHERE d."NAME" = 'sample_db'
   AND t."TBL_NAME" = 'duck_fixture_nonstd'
ON CONFLICT ("PART_ID", "PARAM_KEY") DO UPDATE SET "PARAM_VALUE" = EXCLUDED."PARAM_VALUE";

SELECT p."PART_NAME", v."INTEGER_IDX", v."PART_KEY_VAL", s."LOCATION"
  FROM "PARTITIONS" p
  JOIN "PARTITION_KEY_VALS" v ON v."PART_ID" = p."PART_ID"
  JOIN "SDS" s ON s."SD_ID" = p."SD_ID"
  JOIN "TBLS" t ON t."TBL_ID" = p."TBL_ID"
 WHERE t."TBL_NAME" = 'duck_fixture_nonstd'
 ORDER BY p."PART_NAME", v."INTEGER_IDX";
SQL
