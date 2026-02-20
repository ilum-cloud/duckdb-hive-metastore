//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_table_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/hms_catalog_set.hpp"
#include "storage/hms_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class UCResult;
class HMSSchemaEntry;

class HMSTableSet : public HMSInSchemaSet {
public:
	explicit HMSTableSet(HMSSchemaEntry &schema);
	~HMSTableSet() override = default;

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

	unique_ptr<HMSTableInfo> GetTableInfo(ClientContext &context, HMSSchemaEntry &schema, const string &table_name);
	optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);

	void AlterTable(ClientContext &context, AlterTableInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

	static void AddColumn(ClientContext &context, UCResult &result, HMSTableInfo &table_info, idx_t column_offset = 0);
};

} // namespace duckdb
