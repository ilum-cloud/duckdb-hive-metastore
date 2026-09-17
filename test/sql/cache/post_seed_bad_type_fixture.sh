#!/usr/bin/env bash
set -euo pipefail

# Fixture for test/sql/cache/error_isolation.test.
#
# Creates sample_db.duck_fixture_bad_type, a text table with a column type this extension cannot map to
# DuckDB (uniontype). Looking it up must fail with that mapping error while every other table in sample_db
# keeps resolving. The duck_ prefix keeps the table out of the listing assertions of other tests.
#
# Idempotent (CREATE TABLE IF NOT EXISTS). Must be run after `make test-env-start`. The Hive CLI needs
# writable scratch directories inside the metastore container.

cd "$(dirname "$0")/../.."

docker compose exec -T hive-metastore hive \
  --hiveconf hive.exec.scratchdir=/tmp/hive_fixture_scratch \
  --hiveconf hive.exec.local.scratchdir=/tmp/hive_fixture_local_scratch \
  --hiveconf hive.downloaded.resources.dir=/tmp/hive_fixture_resources \
  -e "CREATE TABLE IF NOT EXISTS sample_db.duck_fixture_bad_type (id int, u uniontype<int,string>) STORED AS TEXTFILE LOCATION 's3a://test-bucket/duck_fixture_bad_type'"
