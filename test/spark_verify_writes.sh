#!/usr/bin/env bash
# Verify that tables written by the DuckDB HMS extension are readable by Spark.
#
# Assumes:
#   - test/docker-compose.yml stack is up (see `make test-env-start`)
#   - the DuckDB sqllogictest suite has already run, so the duck_* tables exist
#
# Runs spark_verify_writes.py inside a one-shot container that reuses the
# seeder image (which already has PySpark + Hadoop S3A + Hive metastore client
# wired up) and joins the test docker network so it can reach hive-metastore
# and minio by service name.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NETWORK="${NETWORK:-test_default}"
IMAGE="${IMAGE:-hive-metastore-test-spark-seeder:latest}"

# Resolve the actual docker network — compose names it after the project dir
# (which is "test" in our case but could be overridden via COMPOSE_PROJECT_NAME).
if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
    CANDIDATE=$(docker network ls --format '{{.Name}}' | grep -E '_default$' | head -1 || true)
    if [[ -n "$CANDIDATE" ]]; then
        echo "Note: network '$NETWORK' not found, falling back to '$CANDIDATE'"
        NETWORK="$CANDIDATE"
    else
        echo "ERROR: could not find a docker compose network. Is the test env up?" >&2
        exit 1
    fi
fi

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "ERROR: image $IMAGE not found. Run 'make test-env-start' first." >&2
    exit 1
fi

exec docker run --rm \
    --network "$NETWORK" \
    -v "${SCRIPT_DIR}/spark_verify_writes.py:/opt/spark/work-dir/spark_verify_writes.py:ro" \
    --entrypoint python3 \
    "$IMAGE" \
    /opt/spark/work-dir/spark_verify_writes.py
