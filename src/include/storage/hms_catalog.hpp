//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/mutex.hpp"
#include "storage/hms_schema_set.hpp"
#include "duckdb/main/attached_database.hpp"
#include "hms_client.hpp"

#include <chrono>

namespace duckdb {
class HMSSchemaEntry;

//! How the files of a partitioned table are located (ATTACH option PARTITION_MODE)
enum class HMSPartitionMode : uint8_t {
	//! Use the partitions registered in the metastore; fall back to the table location when there are none
	AUTO,
	//! Always use the partitions registered in the metastore; error when a partitioned table has none
	HMS,
	//! Never read the partition list; glob the table location and read partition values from key=value paths
	PATH
};

class HMSClearCacheFunction : public TableFunction {
public:
	HMSClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};

class HMSCatalog : public Catalog {
public:
	//! How long metadata loaded from the metastore is used before it is revalidated (ATTACH option METADATA_CACHE_TTL)
	static constexpr idx_t DEFAULT_METADATA_CACHE_TTL_SECONDS = 5;

	explicit HMSCatalog(AttachedDatabase &db_p, const string &internal_name, AttachOptions &attach_options,
	                    string endpoint, const string &default_schema, const string &warehouse_location = "",
	                    string catalog_name = "hive_metastore",
	                    idx_t metadata_cache_ttl_seconds = DEFAULT_METADATA_CACHE_TTL_SECONDS,
	                    HMSPartitionMode partition_mode = HMSPartitionMode::AUTO);
	~HMSCatalog() override;

	string internal_name;
	AccessMode access_mode;
	string endpoint;
	string warehouse_location;

	string catalog_name;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return catalog_name;
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	ErrorData SupportsCreateTable(BoundCreateTableInfo &info) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	string GetDefaultSchema() const override;

	//! Whether or not this is an in-memory UC database
	bool InMemory() override;
	string GetDBPath() override;

	std::chrono::steady_clock::duration GetMetadataCacheTTL() const {
		return metadata_cache_ttl;
	}
	HMSPartitionMode GetPartitionMode() const {
		return partition_mode;
	}
	//! Incremented by ClearCache; catalog sets compare it to invalidate what they cached earlier
	idx_t GetCacheGeneration() const {
		return cache_generation.load();
	}
	//! Names of the tables in a schema, for "did you mean" suggestions (no table is loaded)
	vector<string> GetTableNamesForSuggestions(ClientContext &context, const string &schema_name);
	//! Marks all cached metadata stale, so the next lookups revalidate against the metastore
	void ClearCache();

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	HMSSchemaSet schemas;
	string default_schema;
	std::chrono::steady_clock::duration metadata_cache_ttl;
	HMSPartitionMode partition_mode;
	atomic<idx_t> cache_generation;

	mutex suggestion_lock;
	case_insensitive_map_t<vector<string>> suggestion_names;
	bool suggestion_names_loaded = false;
	idx_t suggestion_names_generation = 0;
	std::chrono::steady_clock::time_point suggestion_names_loaded_at;
};

} // namespace duckdb
