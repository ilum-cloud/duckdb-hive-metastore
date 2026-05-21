PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckdb_hive_metastore
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Core extensions that we need for crucial testing
# IF you have a better idea how to add delta + avro + iceberg here, please suggest and fire a PR
DEFAULT_TEST_EXTENSION_DEPS=parquet;httpfs

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Hive Metastore integration test targets
.PHONY: test-all test-env-start test-env-stop test-run test-mutate-oss test-schema-drift spark-verify-writes test-spark-verify test-cross-engine-scenarios test-cross-engine-scenarios-run

# Main target: build, start env, run tests, stop env
test-all: release
	@echo "========================================"
	@echo "Running Hive Metastore Integration Tests"
	@echo "========================================"
	@echo ""
	@echo "[1/6] Cleaning up any old containers..."
	cd test && docker compose down -v --remove-orphans 2>/dev/null || true
	@echo ""
	@echo "[2/6] Starting Hive Metastore test environment..."
	cd test && docker compose up -d
	@echo ""
	@echo "[3/6] Waiting for Hive Metastore to be ready and seeded..."
	cd test && docker compose wait spark-seeder
	@echo "✓ Hive Metastore is ready and seeded with data"
	@echo ""
	@echo "[4/6] Applying oss:// metastore mutation for scheme-rewrite test..."
	bash test/sql/oss/post_seed_oss_mutation.sh
	@echo ""
	@echo "[5/6] Applying schema-drift mutation for schema_drift test..."
	bash test/sql/oss/post_seed_schema_drift.sh
	@echo ""
	@echo "[6/6] Running tests..."
	HMS_TEST_AVAILABLE=1 ./build/release/test/unittest 'test*'
	@echo ""
	@echo "========================================"
	@echo "Tests completed. Cleaning up..."
	@echo "========================================"
	cd test && docker compose down -v

# Start the test environment (manual control)
test-env-start:
	@echo "Starting Hive Metastore test environment..."
	cd test && docker compose up -d
	@echo "Waiting for Hive Metastore to be ready and seeded..."
	cd test && docker compose wait spark-seeder
	@echo "✓ Hive Metastore is ready and seeded with data"

# Stop the test environment
test-env-stop:
	@echo "Stopping Hive Metastore test environment..."
	cd test && docker compose down -v

# Run tests only (assumes env is already started)
test-run:
	@echo "Running tests..."
	HMS_TEST_AVAILABLE=1 ./build/release/test/unittest 'test*'

# Flip selected HMS table locations to oss:// (idempotent). Must be run after
# the test env is up; required for test/sql/oss/data_access_oss_scheme.test.
test-mutate-oss:
	bash test/sql/oss/post_seed_oss_mutation.sh

# Apply a post-seed schema mutation (adds `drift_note` column to customers).
# Idempotent. Run after test-env-start; required for
# test/sql/oss/schema_drift.test.
test-schema-drift:
	bash test/sql/oss/post_seed_schema_drift.sh

# Cross-engine verification: Spark reads back the tables DuckDB wrote.
# Requires the docker-compose env to be running AND the DuckDB tests to have
# already populated the duck_* tables.
spark-verify-writes:
	bash test/spark_verify_writes.sh

# Full Spark cross-engine verification: bring env up, run DuckDB tests so
# the duck_* tables get populated, then verify via Spark, then tear down.
# Runs the verifier in `bidirectional` mode so the Spark write path is
# exercised on every CI run (Parquet + CSV append from Spark, re-verify count).
# Used by the spark-verify CI job.
test-spark-verify: release
	@echo "========================================"
	@echo "Running Spark Cross-Engine Verification"
	@echo "========================================"
	@echo ""
	@echo "[1/7] Cleaning up any old containers..."
	cd test && docker compose down -v --remove-orphans 2>/dev/null || true
	@echo ""
	@echo "[2/7] Starting Hive Metastore test environment..."
	cd test && docker compose up -d
	@echo ""
	@echo "[3/7] Waiting for Hive Metastore to be ready and seeded..."
	cd test && docker compose wait spark-seeder
	@echo "✓ Hive Metastore is ready and seeded with data"
	@echo ""
	@echo "[4/7] Applying post-seed HMS mutations..."
	bash test/sql/oss/post_seed_oss_mutation.sh
	bash test/sql/oss/post_seed_schema_drift.sh
	@echo ""
	@echo "[5/7] Running DuckDB tests (populates duck_* tables)..."
	HMS_TEST_AVAILABLE=1 ./build/release/test/unittest 'test*'
	@echo ""
	@echo "[6/7] Running insert_scenarios.sql (populates xverify_* tables)..."
	$(MAKE) test-cross-engine-scenarios-run
	@echo ""
	@echo "[7/7] Verifying via Spark (bidirectional mode: read + Spark INSERT)..."
	VERIFY_MODE=bidirectional bash test/spark_verify_writes.sh
	@echo ""
	@echo "========================================"
	@echo "Spark verification complete. Cleaning up..."
	@echo "========================================"
	cd test && docker compose down -v

# Run insert_scenarios.sql against an already-running env. Used as a sub-step
# of test-spark-verify and as a standalone debugging tool. Assumes:
#   - test env is up (docker compose, MinIO + HMS)
#   - ./build/release/duckdb exists (the CLI shipped with the unittest build)
#   - the hive_metastore extension binary has been built
# Wired into the CI Tier-3 spark-verify job via test-spark-verify above.
test-cross-engine-scenarios-run:
	printf "LOAD '%sbuild/release/extension/hive_metastore/hive_metastore.duckdb_extension';\n.read %stest/spark_verify/insert_scenarios.sql\n" \
	    "$(PROJ_DIR)" "$(PROJ_DIR)" \
	  | ./build/release/duckdb -unsigned

# Standalone entry point: build, bring env up, run scenarios, Spark-verify, tear
# down. Mirrors test-spark-verify but only exercises the scenarios path. Useful
# for hand-running cross-engine debugging without the full DuckDB sqllogictest
# suite.
test-cross-engine-scenarios: release
	@echo "========================================"
	@echo "Running cross-engine insert scenarios"
	@echo "========================================"
	cd test && docker compose down -v --remove-orphans 2>/dev/null || true
	cd test && docker compose up -d
	cd test && docker compose wait spark-seeder
	$(MAKE) test-cross-engine-scenarios-run
	bash test/spark_verify_writes.sh
	cd test && docker compose down -v

# Override tidy-check to ensure Thrift files are generated first
.PHONY: tidy-check
tidy-check:
	mkdir -p ./build/tidy
	chmod +x duckdb/scripts/apply_extension_patches.py
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) -DDISABLE_UNITY=1 -DCLANG_TIDY=1 -S $(DUCKDB_SRCDIR) -B build/tidy
	cmake --build build/tidy --target hms_thrift_lib -j$(shell nproc)
	cp duckdb/.clang-tidy build/tidy/.clang-tidy
	cd build/tidy && python3 ../../duckdb/scripts/run-clang-tidy.py '$(PROJ_DIR)src/.*/' -header-filter '$(PROJ_DIR)src/.*/' -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS}
