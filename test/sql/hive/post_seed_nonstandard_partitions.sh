#!/usr/bin/env bash
set -euo pipefail

# Fixture for the partition tests in test/sql/hive/.
#
# Creates two partitioned Parquet tables that Spark cannot produce:
#   * duck_fixture_nonstd  - partition directories do NOT follow Hive's key=value convention, one
#                            partition is stored outside the table location, one partition holds the
#                            NULL marker value (__HIVE_DEFAULT_PARTITION__), and one holds a data
#                            file named the way Hive names them, without an extension.
#   * duck_fixture_noparts - declares partition columns but has no partition registered at all.
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

SELECT p."PART_NAME", v."INTEGER_IDX", v."PART_KEY_VAL", s."LOCATION"
  FROM "PARTITIONS" p
  JOIN "PARTITION_KEY_VALS" v ON v."PART_ID" = p."PART_ID"
  JOIN "SDS" s ON s."SD_ID" = p."SD_ID"
  JOIN "TBLS" t ON t."TBL_ID" = p."TBL_ID"
 WHERE t."TBL_NAME" = 'duck_fixture_nonstd'
 ORDER BY p."PART_NAME", v."INTEGER_IDX";
SQL
