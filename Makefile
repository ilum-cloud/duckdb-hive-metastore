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
.PHONY: test-all test-env-start test-env-stop test-run

# Main target: build, start env, run tests, stop env
test-all: release
	@echo "========================================"
	@echo "Running Hive Metastore Integration Tests"
	@echo "========================================"
	@echo ""
	@echo "[1/4] Cleaning up any old containers..."
	cd test && docker compose down -v --remove-orphans 2>/dev/null || true
	@echo ""
	@echo "[2/4] Starting Hive Metastore test environment..."
	cd test && docker compose up -d
	@echo ""
	@echo "[3/4] Waiting for Hive Metastore to be ready and seeded..."
	cd test && docker compose wait spark-seeder
	@echo "✓ Hive Metastore is ready and seeded with data"
	@echo ""
	@echo "[4/4] Running tests..."
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

# Override tidy-check to ensure Thrift files are generated first
.PHONY: tidy-check
tidy-check:
	mkdir -p ./build/tidy
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) -DDISABLE_UNITY=1 -DCLANG_TIDY=1 -S $(DUCKDB_SRCDIR) -B build/tidy
	cmake --build build/tidy --target hms_thrift_lib -j$(shell nproc)
	cp duckdb/.clang-tidy build/tidy/.clang-tidy
	cd build/tidy && python3 ../../duckdb/scripts/run-clang-tidy.py '$(PROJ_DIR)src/.*/' -header-filter '$(PROJ_DIR)src/.*/' -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS}
