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
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
