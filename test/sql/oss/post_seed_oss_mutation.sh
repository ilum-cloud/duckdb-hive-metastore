#!/usr/bin/env bash
set -euo pipefail

# Fixture for test/sql/oss/data_access_oss_scheme.test.
#
# Flips selected HMS table locations from s3a:// to oss:// so the OSS rewrite
# codepath in PathUtils::NormalizeScanPath is exercised end-to-end. The
# underlying Parquet files in MinIO are NOT moved -- only HMS metadata changes.
#
# Idempotent: re-running leaves the locations as oss://... (REPLACE finds
# nothing to substitute on the second pass).
#
# Must be run after `make test-env-start` so hive-metastore-postgresql is up.
# Resolves the test/ directory relative to this script so `docker compose exec`
# finds docker-compose.yml regardless of the caller's cwd.

cd "$(dirname "$0")/../.."

docker compose exec -T hive-metastore-postgresql \
  psql -U hive -d metastore -v ON_ERROR_STOP=1 <<'SQL'
UPDATE "SDS"
   SET "LOCATION" = REPLACE("LOCATION", 's3a://test-bucket/', 'oss://test-bucket/')
 WHERE "SD_ID" IN (
   SELECT t."SD_ID"
     FROM "TBLS" t
     JOIN "DBS"  d ON d."DB_ID" = t."DB_ID"
    WHERE d."NAME" = 'sample_db'
      AND t."TBL_NAME" IN ('customers', 'reviews')
 );

UPDATE "SDS"
   SET "LOCATION" = REPLACE("LOCATION", 's3a://test-bucket/', 'oss://test-bucket/')
 WHERE "SD_ID" IN (
   SELECT p."SD_ID"
     FROM "PARTITIONS" p
     JOIN "TBLS" t ON t."TBL_ID" = p."TBL_ID"
     JOIN "DBS"  d ON d."DB_ID" = t."DB_ID"
    WHERE d."NAME" = 'sample_db'
      AND t."TBL_NAME" = 'reviews'
 );

SELECT t."TBL_NAME", s."LOCATION"
  FROM "TBLS" t
  JOIN "SDS"  s ON s."SD_ID" = t."SD_ID"
  JOIN "DBS"  d ON d."DB_ID" = t."DB_ID"
 WHERE d."NAME" = 'sample_db'
   AND t."TBL_NAME" IN ('customers', 'reviews')
 ORDER BY t."TBL_NAME";
SQL
