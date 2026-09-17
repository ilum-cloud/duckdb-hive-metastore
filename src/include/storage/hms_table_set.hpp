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
class HMSSchemaEntry;

class HMSTableSet : public HMSCatalogSet {
public:
	explicit HMSTableSet(HMSSchemaEntry &schema);
	~HMSTableSet() override = default;

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

	void AlterTable(ClientContext &context, AlterTableInfo &info);

	void DropEntry(ClientContext &context, DropInfo &info) override;

protected:
	bool SupportsPointLookup() const override {
		return true;
	}
	HMSLoadResult LoadEntry(ClientContext &context, const string &name, optional_ptr<CatalogEntry> cached) override;
	vector<string> ListEntryNames(ClientContext &context) override;
	void LoadEntries(ClientContext &context, const vector<pair<string, optional_ptr<CatalogEntry>>> &requests,
	                 const std::function<void(const string &name, HMSLoadResult result)> &on_loaded) override;

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

private:
	//! Decides whether the table as currently stored in the metastore replaces the cached entry
	HMSLoadResult Revalidate(ClientContext &context, HMSAPITable table, optional_ptr<CatalogEntry> cached);

	HMSSchemaEntry &schema;
};

} // namespace duckdb
