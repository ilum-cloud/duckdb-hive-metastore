//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "hms_lineage.hpp"
#include "storage/hms_partition_plan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include <chrono>

namespace duckdb {

// Forward declarations to avoid including Thrift-dependent headers
struct HMSAPITable;
struct HMSAPIColumnDefinition;
namespace hms {
struct FormatDetectionResult;
} // namespace hms

//! Where the columns of a table entry come from
enum class HMSSchemaSource : uint8_t {
	//! Discovered from the data files (Parquet/Delta/Iceberg)
	FILES,
	//! The Spark schema stored in the table parameters
	SPARK_SCHEMA,
	//! The metastore column definitions
	HMS_COLUMNS
};

class HMSTableEntry : public TableCatalogEntry {
public:
	HMSTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);

	//! Builds the entry for a table as stored in the metastore. Columns are discovered from the data files for
	//! Parquet/Delta/Iceberg tables, otherwise taken from the Spark schema or the metastore columns.
	static unique_ptr<HMSTableEntry> Build(ClientContext &context, Catalog &catalog, SchemaCatalogEntry &schema,
	                                       HMSAPITable table);

	//! Whether both entries have the same column names and types, in the same order
	bool HasSameColumns(const HMSTableEntry &other) const;

	//! Whether the scan of this table fills the partition columns itself, which is also what makes the entry carry
	//! them. Decided from the metastore metadata alone, so it never costs a request.
	static bool InjectsPartitionColumns(const HMSAPITable &table, const hms::FormatDetectionResult &format);

	//! How to locate this table's files and fill its partition columns. The partition list is fetched on the first
	//! scan and reused for the catalog's metadata cache TTL. Never called while looking up or listing tables.
	shared_ptr<const HMSPartitionPlan> GetPartitionPlan(ClientContext &context);

	unique_ptr<HMSAPITable> table_data;
	HMSSchemaSource schema_source = HMSSchemaSource::HMS_COLUMNS;

	shared_ptr<AttachedDatabase> internal_attached_database;

public:
	optional_ptr<Catalog> GetInternalCatalog();
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	// Helper to discover schema for Parquet/Delta/Iceberg tables
	// For Parquet: discovers data columns from files, merges with HMS partition columns
	// For Delta/Iceberg: discovers full schema from table metadata
	static bool DiscoverDynamicSchema(ClientContext &context, Catalog &catalog, SchemaCatalogEntry &schema,
	                                  HMSAPITable &table_data, vector<ColumnDefinition> &columns);

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;

	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	//! Get lineage metadata for this HMS table
	//! Returns lineage information including storage location, table type, etc.
	HMSLineageInfo GetLineageInfo() const;

private:
	mutex partition_lock;
	shared_ptr<const HMSPartitionPlan> partition_plan;
	std::chrono::steady_clock::time_point partition_plan_loaded_at;
	idx_t partition_plan_generation = 0;
};

} // namespace duckdb
