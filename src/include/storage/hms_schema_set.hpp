//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/hms_catalog_set.hpp"
#include "storage/hms_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class HMSSchemaSet : public HMSCatalogSet {
public:
	explicit HMSSchemaSet(Catalog &catalog);
	~HMSSchemaSet() override = default;

public:
	optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);

protected:
	bool SupportsPointLookup() const override {
		return false;
	}
	vector<string> ListEntryNames(ClientContext &context) override;
	void LoadEntries(ClientContext &context, const vector<pair<string, optional_ptr<CatalogEntry>>> &requests,
	                 const std::function<void(const string &name, HMSLoadResult result)> &on_loaded) override;
};

} // namespace duckdb
