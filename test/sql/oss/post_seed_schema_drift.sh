#!/usr/bin/env bash
set -euo pipefail

# Fixture for test/sql/oss/schema_drift.test.
#
# Adds a 'drift_note' (string) column to sample_db.customers in HMS, simulating
# a schema evolution that occurs after DuckDB has already attached the catalog.
# Only HMS metadata is changed -- the underlying Parquet files are untouched, so
# existing rows will return NULL for the new column.
#
# Idempotent: the INSERT is guarded by a WHERE NOT EXISTS check; re-running is a
# no-op if 'drift_note' already exists for the table's CD_ID.
#
# Must be run after `make test-env-start` so hive-metastore-postgresql is up.
# Resolves the test/ directory relative to this script so `docker compose exec`
# finds docker-compose.yml regardless of the caller's cwd.

cd "$(dirname "$0")/../.."

docker compose exec -T hive-metastore-postgresql \
  psql -U hive -d metastore -v ON_ERROR_STOP=1 <<'SQL'
DO $$
DECLARE
  v_cd_id   BIGINT;
  v_max_idx INTEGER;
BEGIN
  SELECT s."CD_ID"
    INTO v_cd_id
    FROM "TBLS" t
    JOIN "SDS"  s ON s."SD_ID" = t."SD_ID"
    JOIN "DBS"  d ON d."DB_ID" = t."DB_ID"
   WHERE d."NAME" = 'sample_db' AND t."TBL_NAME" = 'customers';

  SELECT MAX("INTEGER_IDX")
    INTO v_max_idx
    FROM "COLUMNS_V2"
   WHERE "CD_ID" = v_cd_id;

  INSERT INTO "COLUMNS_V2" ("CD_ID", "COLUMN_NAME", "TYPE_NAME", "INTEGER_IDX", "COMMENT")
  SELECT v_cd_id, 'drift_note', 'string', v_max_idx + 1, NULL
   WHERE NOT EXISTS (
     SELECT 1 FROM "COLUMNS_V2"
      WHERE "CD_ID" = v_cd_id AND "COLUMN_NAME" = 'drift_note'
   );
END;
$$;

SELECT "COLUMN_NAME", "TYPE_NAME", "INTEGER_IDX"
  FROM "COLUMNS_V2"
 WHERE "CD_ID" = (
   SELECT s."CD_ID"
     FROM "TBLS" t
     JOIN "SDS"  s ON s."SD_ID" = t."SD_ID"
     JOIN "DBS"  d ON d."DB_ID" = t."DB_ID"
    WHERE d."NAME" = 'sample_db' AND t."TBL_NAME" = 'customers'
 )
 ORDER BY "INTEGER_IDX";
SQL
