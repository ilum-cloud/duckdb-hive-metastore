//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "hms_lineage.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

// Forward declarations to avoid including Thrift-dependent headers
struct HMSAPITable;
struct HMSAPIColumnDefinition;

struct HMSTableInfo {
	HMSTableInfo() {
		create_info = make_uniq<CreateTableInfo>();
	}
	HMSTableInfo(const string &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>(string(), schema, table);
	}
	HMSTableInfo(const SchemaCatalogEntry &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)schema, table);
	}

	const string &GetTableName() const {
		return create_info->table;
	}

	unique_ptr<CreateTableInfo> create_info;
	unique_ptr<HMSAPITable> table_data;
};

class HMSTableEntry : public TableCatalogEntry {
public:
	HMSTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
	HMSTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, HMSTableInfo &info);

	unique_ptr<HMSAPITable> table_data;

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
};

} // namespace duckdb
